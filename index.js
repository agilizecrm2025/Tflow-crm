// Importa as bibliotecas necessárias para o projeto
const express = require('express');
const { Pool } = require('pg');
const axios = require('axios');
const crypto = require('crypto');
const cors = require('cors');

// Cria uma instância do Express
const app = express();
app.use(cors());
const port = process.env.PORT || 10000;

// Middleware para entender dados JSON
app.use(express.json({ limit: '50mb' }));

// Função para mapear o evento do CRM para o evento do Facebook
const mapCRMEventToFacebookEvent = (crmEvent) => {
    switch (crmEvent.toUpperCase()) {
        case 'NOVOS': return 'Lead';
        case 'ATENDEU': return 'Atendeu';
        case 'OPORTUNIDADE': return 'Oportunidade';
        case 'AVANÇADO': return 'Avançado';
        case 'VÍDEO': return 'Vídeo';
        case 'VENCEMOS': return 'Vencemos';
        case 'QUER EMPREGO': return 'Desqualificado';
        case 'QUER EMPRESTIMO': return 'Não Qualificado';
        default: return crmEvent;
    }
};

// Cria um Pool de conexões com o banco de dados
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});

// Função para inicializar o banco de dados e adicionar NOVAS COLUNAS
const initializeDatabase = async () => {
    const client = await pool.connect();
    try {
        console.log('Conexão com o pool do banco de dados estabelecida.');
        
        const createTableQuery = `
            CREATE TABLE IF NOT EXISTS leads (
                facebook_lead_id TEXT PRIMARY KEY,
                created_time BIGINT,
                email TEXT,
                phone TEXT,
                first_name TEXT,
                last_name TEXT,
                dob TEXT,
                city TEXT,
                estado TEXT,
                zip_code TEXT,
                ad_id TEXT,
                ad_name TEXT,
                adset_id TEXT,
                adset_name TEXT,
                campaign_id TEXT,
                campaign_name TEXT,
                form_id TEXT,
                form_name TEXT,
                platform TEXT,
                is_organic BOOLEAN,
                lead_status TEXT
            );
        `;
        await client.query(createTableQuery);
        console.log('Tabela "leads" principal verificada/criada com sucesso.');
        
    } catch (err) {
        console.error('Erro ao inicializar o banco de dados:', err.message);
    } finally {
        client.release();
    }
};

initializeDatabase();

// ROTA DE IMPORTAÇÃO ATUALIZADA
app.post('/import-leads', async (req, res) => {
    const leadsToImport = req.body;
    if (!Array.isArray(leadsToImport)) { return res.status(400).send('Formato inválido.'); }

    const client = await pool.connect();
    try {
        await client.query('BEGIN');
        const queryText = `
            INSERT INTO leads (facebook_lead_id, created_time, email, phone, first_name, last_name, dob, city, estado, zip_code, ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name, form_id, form_name, platform, is_organic, lead_status)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
            ON CONFLICT (facebook_lead_id) DO UPDATE SET
                created_time = EXCLUDED.created_time, email = EXCLUDED.email, phone = EXCLUDED.phone,
                first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name, dob = EXCLUDED.dob,
                city = EXCLUDED.city, estado = EXCLUDED.estado, zip_code = EXCLUDED.zip_code,
                ad_id = EXCLUDED.ad_id, ad_name = EXCLUDED.ad_name, adset_id = EXCLUDED.adset_id,
                adset_name = EXCLUDED.adset_name, campaign_id = EXCLUDED.campaign_id, campaign_name = EXCLUDED.campaign_name,
                form_id = EXCLUDED.form_id, form_name = EXCLUDED.form_name, platform = EXCLUDED.platform,
                is_organic = EXCLUDED.is_organic, lead_status = EXCLUDED.lead_status;
        `;
        for (const lead of leadsToImport) {
            // Usa 'id' da planilha como 'facebook_lead_id'
            if (!lead || !lead.id) continue;
            // Converte a data/hora para Unix Timestamp
            const createdTimestamp = lead.created_time ? Math.floor(new Date(lead.created_time).getTime() / 1000) : null;
            await client.query(queryText, [
                lead.id, createdTimestamp, lead.email, (lead.phone_number || '').replace(/\D/g, ''),
                lead.nome, lead.sobrenome, lead.data_de_nascimento, lead.city,
                lead.state, lead.cep, lead.ad_id, lead.ad_name, lead.adset_id,
                lead.adset_name, lead.campaign_id, lead.campaign_name, lead.form_id,
                lead.form_name, lead.platform, lead.is_organic, lead.lead_status
            ]);
        }
        await client.query('COMMIT');
        res.status(201).send('Leads importados com sucesso!');
    } catch (error) {
        await client.query('ROLLBACK');
        console.error('Erro ao importar leads:', error.message);
        res.status(500).send('Erro interno do servidor.');
    } finally {
        client.release();
    }
});

// ENDPOINT DO WEBHOOK ATUALIZADO
app.post('/webhook', async (req, res) => {
    console.log("--- Webhook recebido ---");
    try {
        const leadData = req.body;
        const crmEventName = leadData.tag ? leadData.tag.name : null;
        if (!crmEventName) { return res.status(200).send('Webhook recebido, mas sem nome de evento.'); }

        const facebookEventName = mapCRMEventToFacebookEvent(crmEventName);
        if (!leadData.lead) { return res.status(400).send('Dados do lead ausentes.'); }
        
        const leadEmail = leadData.lead.email ? leadData.lead.email.toLowerCase() : null;
        const leadPhone = leadData.lead.phone ? leadData.lead.phone.replace(/\D/g, '') : null;
        if (!leadEmail && !leadPhone) { return res.status(400).send('E-mail ou telefone ausentes.'); }

        const result = await pool.query('SELECT * FROM leads WHERE email = $1 OR phone = $2', [leadEmail, leadPhone]);

        if (result.rows.length === 0) {
            console.log('Lead não encontrado no banco de dados.');
            return res.status(200).send('ID do Facebook não encontrado.');
        }

        const dbRow = result.rows[0];
        const PIXEL_ID = process.env.PIXEL_ID;
        const FB_ACCESS_TOKEN = process.env.FB_ACCESS_TOKEN;
        if (!PIXEL_ID || !FB_ACCESS_TOKEN) {
            console.error('ERRO: Variáveis de ambiente não configuradas!');
            return res.status(500).send('Erro de configuração no servidor.');
        }

        const userData = {};
        if (dbRow.email) userData.em = [crypto.createHash('sha256').update(dbRow.email).digest('hex')];
        if (dbRow.phone) userData.ph = [crypto.createHash('sha256').update(dbRow.phone).digest('hex')];
        if (dbRow.first_name) userData.fn = [crypto.createHash('sha256').update(dbRow.first_name.toLowerCase()).digest('hex')];
        if (dbRow.last_name) userData.ln = [crypto.createHash('sha256').update(dbRow.last_name.toLowerCase()).digest('hex')];
        if (dbRow.dob) userData.db = [crypto.createHash('sha256').update(String(dbRow.dob).replace(/\D/g, '')).digest('hex')];
        if (dbRow.city) userData.ct = [crypto.createHash('sha256').update(dbRow.city.toLowerCase()).digest('hex')];
        if (dbRow.estado) userData.st = [crypto.createHash('sha256').update(dbRow.estado.toLowerCase()).digest('hex')];
        if (dbRow.zip_code) userData.zp = [crypto.createHash('sha256').update(String(dbRow.zip_code).replace(/\D/g, '')).digest('hex')];
        if (dbRow.facebook_lead_id) userData.lead_id = dbRow.facebook_lead_id;

        // O event_time agora usa o 'created_time' para o evento 'Lead' e o tempo real para os outros.
        const eventTime = (facebookEventName === 'Lead' && dbRow.created_time) ? dbRow.created_time : Math.floor(Date.now() / 1000);

        const eventData = { 
            event_name: facebookEventName, 
            event_time: eventTime, 
            action_source: 'system_generated', 
            user_data: userData,
            custom_data: { 
                event_source: 'crm',
                lead_event_source: 'Greenn Sales',
                campaign_id: dbRow.campaign_id,
                ad_id: dbRow.ad_id,
                adset_id: dbRow.adset_id,
                form_id: dbRow.form_id,
                platform: dbRow.platform,
                is_organic: dbRow.is_organic,
                lead_status: dbRow.lead_status
            }
        };
        const facebookAPIUrl = `https://graph.facebook.com/v24.0/${PIXEL_ID}/events?access_token=${FB_ACCESS_TOKEN}`;
        
        console.log(`Enviando evento '${facebookEventName}' para a API do Facebook...`);
        await axios.post(facebookAPIUrl, { data: [eventData] });

        console.log(`Evento '${facebookEventName}' disparado com sucesso para o lead com ID: ${dbRow.facebook_lead_id}`);
        res.status(200).send('Evento de conversão enviado com sucesso!');

    } catch (error) {
        console.error('Erro ao processar o webhook:', error.response ? JSON.stringify(error.response.data) : error.message);
        res.status(500).send('Erro interno do servidor.');
    }
});

// ROTA DE TESTE E HEALTH CHECK
app.get('/', (req, res) => {
  console.log("A rota principal (GET /) foi acessada com sucesso!");
  res.status(200).send("Servidor no ar e respondendo.");
});

// Inicia o servidor
app.listen(port, () => {
    console.log(`Servidor rodando na porta ${port}`);
});
