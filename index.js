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

        const columns = {
            'first_name': 'TEXT',
            'last_name': 'TEXT',
            'dob': 'TEXT',
            'city': 'TEXT',
            'estado': 'TEXT',
            'zip_code': 'TEXT'
        };

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

// ===================================================================
// ROTAS DE IMPORTAÇÃO RESTAURADAS
// ===================================================================
app.get('/importar', (req, res) => {
    res.send(`
        <!DOCTYPE html>
        <html>
        <head>
            <title>Importar Leads</title>
            <style>
                body { font-family: sans-serif; text-align: center; margin-top: 50px; }
                textarea { width: 800px; height: 300px; margin-top: 20px; font-family: monospace; }
                button { padding: 10px 20px; font-size: 16px; cursor: pointer; }
                h1 { color: #333; }
                p { color: #666; }
            </style>
        </head>
        <body>
            <h1>Importar Leads para o Banco de Dados</h1>
            <p>Cole seus dados JSON aqui. Use as chaves: facebook_lead_id, first_name, last_name, phone, email, dob, city, estado, zip_code.</p>
            <textarea id="leads-data" placeholder='[{"facebook_lead_id": "ID_FACEBOOK", "first_name": "Joao", "last_name": "Silva", "phone": "+5511987654321", "email": "email@exemplo.com", "dob": "19901231", "city": "Sao Paulo", "estado": "SP", "zip_code": "01000000"}]'></textarea><br>
            <button onclick="importLeads()">Importar Leads</button>
            <p id="status-message" style="margin-top: 20px; font-weight: bold;"></p>
            <script>
                async function importLeads() {
                    const data = document.getElementById('leads-data').value;
                    const statusMessage = document.getElementById('status-message');
                    try {
                        const response = await fetch('/import-leads', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: data });
                        const result = await response.text();
                        statusMessage.textContent = result;
                        statusMessage.style.color = 'green';
                    } catch (error) {
                        statusMessage.textContent = 'Erro na importação: ' + error.message;
                        statusMessage.style.color = 'red';
                    }
                }
            </script>
        </body>
        </html>
    `);
});

app.post('/import-leads', async (req, res) => {
    const leadsToImport = req.body;
    if (!Array.isArray(leadsToImport) || leadsToImport.length === 0) {
        return res.status(400).send('Dados de importação ausentes ou formato inválido.');
    }
    try {
        const queryText = `
            INSERT INTO leads (facebook_lead_id, email, phone, first_name, last_name, dob, city, estado, zip_code)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (facebook_lead_id) DO UPDATE SET
                email = EXCLUDED.email, phone = EXCLUDED.phone, first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name, dob = EXCLUDED.dob, city = EXCLUDED.city,
                estado = EXCLUDED.estado, zip_code = EXCLUDED.zip_code;
        `;
        for (const lead of leadsToImport) {
            if (!lead || typeof lead !== 'object' || !lead.facebook_lead_id) continue;
            await pool.query(queryText, [
                lead.facebook_lead_id, lead.email || null, (lead.phone || '').replace(/\D/g, ''),
                lead.first_name || null, lead.last_name || null, lead.dob || null,
                lead.city || null, lead.estado || null, lead.zip_code || null
            ]);
        }
        res.status(201).send('Leads importados com sucesso!');
    } catch (error) {
        console.error('Erro ao importar leads:', error.message);
        res.status(500).send('Erro interno do servidor.');
    }
});
// ===================================================================

// ENDPOINT DO WEBHOOK: Onde o CRM envia o evento
app.post('/webhook', async (req, res) => {
    console.log("--- Webhook recebido ---");
    try {
        const leadData = req.body;
        const crmEventName = leadData.tag ? leadData.tag.name : null;
        if (!crmEventName) {
            console.log('Webhook recebido, mas sem nome de evento válido.');
            return res.status(200).send('Webhook recebido, mas sem nome de evento.');
        }

        const facebookEventName = mapCRMEventToFacebookEvent(crmEventName);
        console.log(`Evento do CRM '${crmEventName}' mapeado para '${facebookEventName}'`);

        if (!leadData || !leadData.lead) {
            console.log('Dados do lead ausentes no webhook.');
            return res.status(400).send('Dados do lead ausentes no webhook.');
        }

        const leadEmail = leadData.lead.email ? leadData.lead.email.toLowerCase() : null;
        const leadPhone = leadData.lead.phone ? leadData.lead.phone.replace(/\D/g, '') : null;
        if (!leadEmail && !leadPhone) {
            console.log('E-mail ou telefone do lead ausentes no webhook.');
            return res.status(400).send('E-mail ou telefone do lead ausentes no webhook.');
        }

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
        if(!PIXEL_ID || !FB_ACCESS_TOKEN) {
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

        const eventData = { event_name: facebookEventName, event_time: Math.floor(Date.now() / 1000), action_source: 'system_generated', user_data: userData, custom_data: { lead_id: dbRow.facebook_lead_id } };
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
