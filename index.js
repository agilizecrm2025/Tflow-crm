// Importa as bibliotecas necessárias para o projeto
const express = require('express');
const { Pool } = require('pg');
const axios = require('axios');
const crypto = require('crypto');
const cors = require('cors');

// Cria uma instância do Express
const app = express();
app.use(cors()); // Habilita o CORS para todas as rotas
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
    ssl: {
        rejectUnauthorized: false
    }
});

// Função para inicializar o banco de dados
const initializeDatabase = async () => {
    try {
        const checkClient = await pool.connect();
        console.log('Conexão com o pool do banco de dados estabelecida.');
        checkClient.release(); 

        await pool.query(`CREATE TABLE IF NOT EXISTS leads (facebook_lead_id TEXT PRIMARY KEY, phone TEXT, email TEXT);`);
        console.log('Tabela "leads" verificada/criada com sucesso.');

        const columns = { 'first_name': 'TEXT', 'last_name': 'TEXT', 'dob': 'TEXT', 'city': 'TEXT', 'estado': 'TEXT', 'zip_code': 'TEXT' };
        for (const [columnName, columnType] of Object.entries(columns)) {
            const check = await pool.query("SELECT 1 FROM information_schema.columns WHERE table_name='leads' AND column_name=$1", [columnName]);
            if (check.rows.length === 0) {
                await pool.query(`ALTER TABLE leads ADD COLUMN ${columnName} ${columnType};`);
                console.log(`Coluna "${columnName}" adicionada à tabela "leads".`);
            }
        }
    } catch (err) {
        console.error('Erro ao inicializar o banco de dados:', err.message);
    }
};

initializeDatabase();

// ENDPOINT DO WEBHOOK: Onde o CRM envia o evento
app.post('/webhook', async (req, res) => {
    console.log("--- Webhook recebido ---");
    try {
        const leadData = req.body;
        const crmEventName = leadData.tag ? leadData.tag.name : null;
        if (!crmEventName) { return res.status(200).send('Webhook recebido, mas sem nome de evento.'); }

        const facebookEventName = mapCRMEventToFacebookEvent(crmEventName);
        console.log(`Evento do CRM '${crmEventName}' mapeado para '${facebookEventName}'`);

        if (!leadData || !leadData.lead) { return res.status(400).send('Dados do lead ausentes no webhook.'); }

        const leadEmail = leadData.lead.email ? leadData.lead.email.toLowerCase() : null;
        const leadPhone = leadData.lead.phone ? leadData.lead.phone.replace(/\D/g, '') : null;
        if (!leadEmail && !leadPhone) { return res.status(400).send('E-mail ou telefone do lead ausentes no webhook.'); }

        console.log(`Buscando no banco por email: ${leadEmail} ou telefone: ${leadPhone}`);
        const result = await pool.query(
            'SELECT facebook_lead_id, first_name, last_name, dob, city, estado, zip_code FROM leads WHERE email = $1 OR phone = $2',
            [leadEmail, leadPhone]
        );

        if (result.rows.length === 0) {
            console.log('Lead não encontrado no banco de dados. Nenhuma ação será tomada.');
            return res.status(200).send('ID do Facebook não encontrado.');
        }

        const dbRow = result.rows[0];
        console.log('Lead encontrado no banco. Preparando evento para o Facebook.');

        const PIXEL_ID = process.env.PIXEL_ID;
        const FB_ACCESS_TOKEN = process.env.FB_ACCESS_TOKEN;
        if (!PIXEL_ID || !FB_ACCESS_TOKEN) {
            console.error('ERRO: Variáveis de ambiente PIXEL_ID ou FB_ACCESS_TOKEN não estão configuradas!');
            return res.status(500).send('Erro de configuração no servidor.');
        }

        const userData = {};
        if (leadEmail) userData.em = [crypto.createHash('sha256').update(leadEmail).digest('hex')];
        if (leadPhone) userData.ph = [crypto.createHash('sha256').update(leadPhone).digest('hex')];
        if (dbRow.first_name) userData.fn = [crypto.createHash('sha256').update(dbRow.first_name.toLowerCase()).digest('hex')];
        if (dbRow.last_name) userData.ln = [crypto.createHash('sha256').update(dbRow.last_name.toLowerCase()).digest('hex')];
        if (dbRow.dob) userData.db = [crypto.createHash('sha256').update(String(dbRow.dob).replace(/\D/g, '')).digest('hex')];
        if (dbRow.city) userData.ct = [crypto.createHash('sha256').update(dbRow.city.toLowerCase()).digest('hex')];
        if (dbRow.estado) userData.st = [crypto.createHash('sha256').update(dbRow.estado.toLowerCase()).digest('hex')];
        if (dbRow.zip_code) userData.zp = [crypto.createHash('sha256').update(String(dbRow.zip_code).replace(/\D/g, '')).digest('hex')];

        // ===================================================================
        // CORREÇÃO: Mover o lead_id para dentro do objeto userData
        // ===================================================================
        if (dbRow.facebook_lead_id) {
            userData.lead_id = dbRow.facebook_lead_id;
        }
        // ===================================================================
        
        const eventData = { 
            event_name: facebookEventName, 
            event_time: Math.floor(Date.now() / 1000), 
            action_source: 'system_generated', 
            user_data: userData,
            custom_data: { 
                event_source: 'crm',
                lead_event_source: 'Your CRM'
            }
        };
        const facebookAPIUrl = `https://graph.facebook.com/v24.0/${PIXEL_ID}/events?access_token=${FB_ACCESS_TOKEN}`;
        
        console.log(`Enviando evento '${facebookEventName}' para a API do Facebook...`);
        await axios.post(facebookAPIUrl, { data: [eventData] });

        console.log(`Evento '${facebookEventName}' disparado com sucesso para o lead com ID: ${dbRow.facebook_lead_id}`);
        res.status(200).send('Evento de conversão enviado com sucesso!');

    } catch (error) {
        if (error.response) {
            console.error('Erro na API do Facebook:', JSON.stringify(error.response.data, null, 2));
        } else {
            console.error('Erro ao processar o webhook:', error.message);
        }
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
