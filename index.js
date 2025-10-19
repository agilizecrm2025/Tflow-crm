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

// Função para inicializar o banco de dados e adicionar TODAS as colunas
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

        // Verifica e adiciona cada coluna individualmente para garantir compatibilidade
        const allColumns = {
            'created_time': 'BIGINT', 'email': 'TEXT', 'phone': 'TEXT', 'first_name': 'TEXT', 'last_name': 'TEXT',
            'dob': 'TEXT', 'city': 'TEXT', 'estado': 'TEXT', 'zip_code': 'TEXT', 'ad_id': 'TEXT', 'ad_name': 'TEXT',
            'adset_id': 'TEXT', 'adset_name': 'TEXT', 'campaign_id': 'TEXT', 'campaign_name': 'TEXT', 'form_id': 'TEXT',
            'form_name': 'TEXT', 'platform': 'TEXT', 'is_organic': 'BOOLEAN', 'lead_status': 'TEXT'
        };

        for (const [columnName, columnType] of Object.entries(allColumns)) {
            const check = await client.query("SELECT 1 FROM information_schema.columns WHERE table_name='leads' AND column_name=$1", [columnName]);
            if (check.rows.length === 0) {
                await client.query(`ALTER TABLE leads ADD COLUMN ${columnName} ${columnType};`);
                console.log(`Coluna de manutenção "${columnName}" adicionada.`);
            }
        }
        
    } catch (err) {
        console.error('Erro ao inicializar o banco de dados:', err.message);
    } finally {
        client.release();
    }
};

initializeDatabase();

// ROTA DE IMPORTAÇÃO (GET) - Para criar a página HTML
app.get('/importar', (req, res) => {
    res.send(`
        <!DOCTYPE html>
        <html>
        <head>
            <title>Importar Leads</title>
            <style> body { font-family: sans-serif; text-align: center; margin-top: 50px; } textarea { width: 90%; max-width: 1200px; height: 400px; margin-top: 20px; font-family: monospace; } button { padding: 10px 20px; font-size: 16px; cursor: pointer; } </style>
        </head>
        <body>
            <h1>Importar Leads para o Banco de Dados</h1>
            <p>Cole seus dados JSON aqui. Use os cabeçalhos da sua planilha (ex: id, created_time, email, etc.).</p>
            <textarea id="leads-data" placeholder='[{"id": "123...", "created_time": "2025-10-20T10:30:00-0300", "email": "teste@email.com", ...}]'></textarea><br>
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

// ROTA DE IMPORTAÇÃO (POST) - Para processar os dados
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
            if (!lead || !lead.id) continue;
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

// ENDPOINT DO WEBHOOK
app.post('/webhook', async (req, res) => {
    // Esta é a mesma lógica do outro projeto. Adapte se necessário.
    console.log("--- Webhook recebido ---");
    try {
        // Lógica do webhook aqui...
        res.status(200).send("Webhook recebido, lógica a ser implementada.");
    } catch (error) {
        console.error('Erro ao processar o webhook:', error.message);
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
