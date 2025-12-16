// ============================================================================
// SERVIDOR DE INTELIGÊNCIA DE LEADS (V8.4 - CORREÇÃO VALUE 0)
// ============================================================================

const express = require('express');
const { Pool } = require('pg');
const axios = require('axios');
const crypto = require('crypto');
const cors = require('cors');

const app = express();

app.use(cors());
const port = process.env.PORT || 10000;
app.use(express.json({ limit: '50mb' }));

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// ============================================================================
// 1. CONFIGURAÇÕES
// ============================================================================

const mapCRMEventToFacebookEvent = (crmEvent) => {
    if (!crmEvent) return 'Lead'; 
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

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});

const initializeDatabase = async () => {
    const client = await pool.connect();
    try {
        console.log('🔄 Verificando estrutura do Banco de Dados...');
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
                lead_status TEXT,
                fbc TEXT, 
                fbp TEXT,
                client_ip_address TEXT, 
                client_user_agent TEXT 
            );
        `;
        await client.query(createTableQuery);

        const allColumns = {
            'created_time': 'BIGINT', 'email': 'TEXT', 'phone': 'TEXT', 'first_name': 'TEXT', 'last_name': 'TEXT',
            'dob': 'TEXT', 'city': 'TEXT', 'estado': 'TEXT', 'zip_code': 'TEXT', 'ad_id': 'TEXT', 'ad_name': 'TEXT',
            'adset_id': 'TEXT', 'adset_name': 'TEXT', 'campaign_id': 'TEXT', 'campaign_name': 'TEXT', 'form_id': 'TEXT',
            'form_name': 'TEXT', 'platform': 'TEXT', 'is_organic': 'BOOLEAN', 'lead_status': 'TEXT',
            'fbc': 'TEXT', 'fbp': 'TEXT',
            'client_ip_address': 'TEXT',
            'client_user_agent': 'TEXT'
        };

        for (const [columnName, columnType] of Object.entries(allColumns)) {
            const check = await client.query("SELECT 1 FROM information_schema.columns WHERE table_name='leads' AND column_name=$1", [columnName]);
            if (check.rows.length === 0) {
                await client.query(`ALTER TABLE leads ADD COLUMN ${columnName} ${columnType};`);
                console.log(`➕ Coluna nova criada: ${columnName}`);
            }
        }
        console.log('✅ Banco de Dados Pronto!');
    } catch (err) {
        console.error('❌ Erro no Banco:', err.message);
    } finally {
        client.release();
    }
};

// ============================================================================
// 2. ROTA: CAPTURA DO SITE
// ============================================================================
app.post('/capture-site-data', async (req, res) => {
    const client = await pool.connect();
    try {
        const data = req.body;
        let ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
        if (ip && ip.includes(',')) ip = ip.split(',')[0].trim();
        const userAgent = data.agent || req.headers['user-agent'];

        console.log(' ');
        console.log('🚀 [SITE] DADO RECEBIDO');
        console.log(`   🆔 ID Sessão: ${data.custom_id}`);
        console.log(`   👤 ${data.name || '-'} | 💻 IP: ${ip}`);

        const webLeadId = data.custom_id || `WEB-${Date.now()}-${Math.floor(Math.random() * 1000)}`;
        const createdTime = Math.floor(Date.now() / 1000);
        const email = data.email ? data.email.toLowerCase().trim() : null;
        const phone = data.phone ? data.phone.replace(/\D/g, '') : null;
        
        let firstName = data.name || '';
        let lastName = '';
        if (firstName.includes(' ')) {
            const parts = firstName.split(' ');
            firstName = parts[0];
            lastName = parts.slice(1).join(' ');
        }

        const queryText = `
            INSERT INTO leads (facebook_lead_id, created_time, email, phone, first_name, last_name, fbc, fbp, client_ip_address, client_user_agent, platform, is_organic, form_name)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'site_smartcred', false, 'Formulario Site')
            ON CONFLICT (facebook_lead_id) DO UPDATE SET
                email = COALESCE(EXCLUDED.email, leads.email),
                phone = COALESCE(EXCLUDED.phone, leads.phone),
                first_name = COALESCE(EXCLUDED.first_name, leads.first_name),
                last_name = COALESCE(EXCLUDED.last_name, leads.last_name),
                fbc = COALESCE(EXCLUDED.fbc, leads.fbc),
                fbp = COALESCE(EXCLUDED.fbp, leads.fbp),
                client_ip_address = COALESCE(EXCLUDED.client_ip_address, leads.client_ip_address),
                client_user_agent = COALESCE(EXCLUDED.client_user_agent, leads.client_user_agent);
        `;

        await client.query(queryText, [
            webLeadId, createdTime, email, phone, firstName, lastName, data.fbc, data.fbp, ip, userAgent
        ]);

        console.log('💾 [DB] Salvo com sucesso!');
        res.status(200).json({ success: true });

    } catch (error) {
        console.error('❌ [ERRO] Falha ao salvar:', error);
        res.status(500).json({ success: false });
    } finally {
        client.release();
    }
});

// ============================================================================
// 3. ROTA: WEBHOOK (CORRIGIDA: REMOVIDO VALUE/CURRENCY)
// ============================================================================
app.post('/webhook', async (req, res) => {
    console.log("--- 🔔 Webhook Recebido ---");
    try {
        const leadData = req.body;
        const crmEventName = leadData.tag ? leadData.tag.name : null;
        if (!crmEventName) return res.status(200).send('Sem tag.');

        const facebookEventName = mapCRMEventToFacebookEvent(crmEventName);
        if (!leadData.lead) return res.status(400).send('Sem dados.');
        
        const leadEmail = leadData.lead.email ? leadData.lead.email.toLowerCase().trim() : null;
        let leadPhone = leadData.lead.phone ? leadData.lead.phone.replace(/\D/g, '') : null;
        if (!leadEmail && !leadPhone) return res.status(400).send('Sem contatos.');

        let searchPhone = leadPhone;
        if (searchPhone && searchPhone.startsWith('55') && searchPhone.length > 11) {
            searchPhone = searchPhone.substring(2);
        }

        let dbRow;
        let result;
        let attempts = 0;
        const searchQuery = `SELECT * FROM leads WHERE email = $1 OR phone LIKE '%' || $2 LIMIT 1`;

        while (attempts < 3) {
            attempts++;
            result = await pool.query(searchQuery, [leadEmail, searchPhone]);
            if (result.rows.length > 0) {
                dbRow = result.rows[0];
                console.log(`✅ Lead encontrado (Tentativa ${attempts})`);
                break; 
            } else {
                if (attempts < 3) await sleep(3000);
            }
        }

        if (!dbRow) {
            console.log('❌ Lead não encontrado no DB.');
            return res.status(200).send('Não encontrado.');
        }

        const PIXEL_ID = process.env.PIXEL_ID;
        const FB_ACCESS_TOKEN = process.env.FB_ACCESS_TOKEN;
        if (!PIXEL_ID || !FB_ACCESS_TOKEN) return res.status(500).send('Erro Config.');

        const userData = {};
        
        if (dbRow.email) userData.em = [crypto.createHash('sha256').update(dbRow.email).digest('hex')];
        if (dbRow.phone) userData.ph = [crypto.createHash('sha256').update(dbRow.phone).digest('hex')];
        if (dbRow.first_name) userData.fn = [crypto.createHash('sha256').update(dbRow.first_name.toLowerCase()).digest('hex')];
        if (dbRow.last_name) userData.ln = [crypto.createHash('sha256').update(dbRow.last_name.toLowerCase()).digest('hex')];
        if (dbRow.city) userData.ct = [crypto.createHash('sha256').update(dbRow.city.toLowerCase()).digest('hex')];
        if (dbRow.estado) userData.st = [crypto.createHash('sha256').update(dbRow.estado.toLowerCase()).digest('hex')];
        if (dbRow.zip_code) userData.zp = [crypto.createHash('sha256').update(String(dbRow.zip_code).replace(/\D/g, '')).digest('hex')];
        if (dbRow.dob) userData.db = [crypto.createHash('sha256').update(String(dbRow.dob).replace(/\D/g, '')).digest('hex')];

        if (dbRow.fbc) userData.fbc = dbRow.fbc;
        if (dbRow.fbp) userData.fbp = dbRow.fbp;
        if (dbRow.client_ip_address) userData.client_ip_address = dbRow.client_ip_address;
        if (dbRow.client_user_agent) userData.client_user_agent = dbRow.client_user_agent;

        if (dbRow.facebook_lead_id) {
            userData.external_id = [crypto.createHash('sha256').update(dbRow.facebook_lead_id).digest('hex')];
        }

        if (dbRow.facebook_lead_id && !dbRow.facebook_lead_id.startsWith('WEB-')) {
            userData.lead_id = dbRow.facebook_lead_id;
        }

        const eventTime = Math.floor(Date.now() / 1000);
        
        // PACOTE DE DADOS SEM VALUE/CURRENCY (CORREÇÃO DO AVISO DO FACEBOOK)
        const eventData = { 
            event_name: facebookEventName, 
            event_time: eventTime, 
            action_source: 'website',
            user_data: userData,
            custom_data: { 
                event_source: 'crm',
                lead_event_source: 'Greenn Sales',
                campaign_name: dbRow.campaign_name,
                form_name: dbRow.form_name,
                lead_status: dbRow.lead_status
                // Value e Currency removidos intencionalmente
            }
        };

        const facebookAPIUrl = `https://graph.facebook.com/v24.0/${PIXEL_ID}/events?access_token=${FB_ACCESS_TOKEN}`;
        
        console.log(`📤 Enviando '${facebookEventName}'...`);
        await axios.post(facebookAPIUrl, { data: [eventData] });

        console.log(`✅ SUCESSO!`);
        res.status(200).send('Enviado.');

    } catch (error) {
        console.error('❌ Erro Webhook:', error.response ? JSON.stringify(error.response.data) : error.message);
        res.status(500).send('Erro.');
    }
});

// ============================================================================
// 4. ROTA DE BACKUP CSV
// ============================================================================
app.get('/baixar-backup', async (req, res) => {
    try {
        const client = await pool.connect();
        const result = await client.query('SELECT * FROM leads');
        client.release();

        if (result.rows.length === 0) return res.send('Banco vazio.');

        const headers = Object.keys(result.rows[0]);
        const csvRows = [];
        csvRows.push(headers.join(','));

        for (const row of result.rows) {
            const values = headers.map(header => {
                const val = row[header];
                const escaped = ('' + (val || '')).replace(/"/g, '""');
                return `"${escaped}"`;
            });
            csvRows.push(values.join(','));
        }

        res.setHeader('Content-Type', 'text/csv');
        res.setHeader('Content-Disposition', 'attachment; filename="backup_leads.csv"');
        res.status(200).send(csvRows.join('\n'));

    } catch (error) {
        console.error('Erro CSV:', error);
        res.status(500).send('Erro ao gerar backup.');
    }
});

// ============================================================================
// 5. ROTAS DE IMPORTAÇÃO
// ============================================================================
app.get('/importar', (req, res) => {
    res.send(`
        <!DOCTYPE html><html><head><title>Importar Leads</title><style>body{font-family:sans-serif;text-align:center;margin-top:50px}textarea{width:90%;max-width:1200px;height:400px;margin-top:20px}button{padding:10px 20px;font-size:16px;cursor:pointer}</style></head>
        <body><h1>Importar Leads</h1><p>Cole o JSON.</p>
        <textarea id="leads-data"></textarea><br><button onclick="importLeads()">Importar</button><p id="status-message"></p>
        <script>
            async function importLeads(){
                const d=document.getElementById('leads-data').value;
                const s=document.getElementById('status-message');
                try{const r=await fetch('/import-leads',{method:'POST',headers:{'Content-Type':'application/json'},body:d});
                const t=await r.text();s.textContent=t;s.style.color='green'}catch(e){s.textContent='Erro: '+e.message;s.style.color='red'}
            }
        </script></body></html>
    `);
});

app.post('/import-leads', async (req, res) => {
    const leadsToImport = req.body;
    if (!Array.isArray(leadsToImport)) return res.status(400).send('Formato inválido.');

    const client = await pool.connect();
    try {
        await client.query('BEGIN');
        const queryText = `
            INSERT INTO leads (facebook_lead_id, created_time, email, phone, first_name, last_name, dob, city, estado, zip_code, ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name, form_id, form_name, platform, is_organic, lead_status, fbc, fbp, client_ip_address, client_user_agent)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25)
            ON CONFLICT (facebook_lead_id) DO UPDATE SET
                email = COALESCE(EXCLUDED.email, leads.email),
                phone = COALESCE(EXCLUDED.phone, leads.phone),
                first_name = COALESCE(EXCLUDED.first_name, leads.first_name),
                fbc = COALESCE(EXCLUDED.fbc, leads.fbc),
                fbp = COALESCE(EXCLUDED.fbp, leads.fbp);
        `;
        
        for (const lead of leadsToImport) {
            const id = lead.id || lead.facebook_lead_id;
            if (!id) continue;
            const phoneRaw = lead.phone_number || lead.phone || '';
            const createdTimestamp = lead.created_time ? (String(lead.created_time).length > 10 ? Math.floor(new Date(lead.created_time).getTime() / 1000) : lead.created_time) : null;

            await client.query(queryText, [
                id, createdTimestamp, lead.email, phoneRaw.replace(/\D/g, ''),
                lead.nome || lead.first_name, lead.sobrenome || lead.last_name, lead.data_de_nascimento || lead.dob, 
                lead.city, lead.state || lead.estado, lead.cep || lead.zip_code, 
                lead.ad_id, lead.ad_name, lead.adset_id, lead.adset_name, lead.campaign_id, lead.campaign_name, 
                lead.form_id, lead.form_name, lead.platform, lead.is_organic, lead.lead_status,
                lead.fbc, lead.fbp, lead.client_ip_address, lead.client_user_agent
            ]);
        }
        await client.query('COMMIT');
        res.status(201).send('Importação concluída!');
    } catch (error) {
        await client.query('ROLLBACK');
        console.error('Erro Import:', error.message);
        res.status(500).send('Erro interno.');
    } finally {
        client.release();
    }
});

// ============================================================================
// 6. INICIALIZAÇÃO
// ============================================================================
app.get('/', (req, res) => res.send('🟢 Servidor V8.4 (Value Fixed) Online!'));

const startServer = async () => {
    try {
        await initializeDatabase();
        app.listen(port, () => console.log(`🚀 Servidor na porta ${port}`));
    } catch (error) {
        console.error("❌ Falha:", error);
    }
};

startServer();
