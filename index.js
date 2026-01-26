// ============================================================================
// SERVIDOR DE INTELIGÊNCIA DE LEADS (V8.22 - BLINDAGEM TRIPLA ANTI-DUPLICIDADE)
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
// 1. CONFIGURAÇÕES E MAPA DE EVENTOS
// ============================================================================

const mapCRMEventToFacebookEvent = (crmEvent) => {
    if (!crmEvent) return null; 
    
    switch (crmEvent.toUpperCase()) {
        case 'NOVOS': return 'Lead'; // Conversão Principal
        case 'ATENDEU': return 'Atendeu';
        case 'OPORTUNIDADE': return 'Oportunidade';
        case 'AVANÇADO': return 'Avançado';
        case 'VÍDEO': return 'Vídeo';
        case 'VENCEMOS': return 'Vencemos'; // Conversão de Venda
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
                client_user_agent TEXT,
                last_sent_event TEXT 
            );
        `;
        // Nota: A coluna 'last_sent_event' foi adicionada acima para a Trava de Estado
        await client.query(createTableQuery);

        const allColumns = {
            'created_time': 'BIGINT', 'email': 'TEXT', 'phone': 'TEXT', 'first_name': 'TEXT', 'last_name': 'TEXT',
            'dob': 'TEXT', 'city': 'TEXT', 'estado': 'TEXT', 'zip_code': 'TEXT', 'ad_id': 'TEXT', 'ad_name': 'TEXT',
            'adset_id': 'TEXT', 'adset_name': 'TEXT', 'campaign_id': 'TEXT', 'campaign_name': 'TEXT', 'form_id': 'TEXT',
            'form_name': 'TEXT', 'platform': 'TEXT', 'is_organic': 'BOOLEAN', 'lead_status': 'TEXT',
            'fbc': 'TEXT', 'fbp': 'TEXT',
            'client_ip_address': 'TEXT',
            'client_user_agent': 'TEXT',
            'last_sent_event': 'TEXT' // <--- COLUNA NOVA (TRAVA 3)
        };

        for (const [columnName, columnType] of Object.entries(allColumns)) {
            const check = await client.query("SELECT 1 FROM information_schema.columns WHERE table_name='leads' AND column_name=$1", [columnName]);
            if (check.rows.length === 0) {
                console.log(`🔧 Criando nova coluna: ${columnName}`);
                await client.query(`ALTER TABLE leads ADD COLUMN ${columnName} ${columnType};`);
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
// 2. ROTA: CAPTURA DO SITE (TRAVA 1 - JANELA 24H)
// ============================================================================
app.post('/capture-site-data', async (req, res) => {
    const client = await pool.connect();
    try {
        const data = req.body;
        let ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
        if (ip && ip.includes(',')) ip = ip.split(',')[0].trim();
        const userAgent = data.agent || req.headers['user-agent'];
        const email = data.email ? data.email.toLowerCase().trim() : null;
        const phone = data.phone ? data.phone.replace(/\D/g, '') : null;
        
        let firstName = data.name || '';
        let lastName = '';
        if (firstName.includes(' ')) {
            const parts = firstName.split(' ');
            firstName = parts[0];
            lastName = parts.slice(1).join(' ');
        }

        console.log(' ');
        console.log(`🚀 [SITE] RECEBIDO: ${firstName} | ${email || 'Sem Email'} | ${phone || 'Sem Fone'}`);

        let webLeadId = null;
        let isNewLead = true;

        // --- TRAVA 1: Verifica se já existe nas últimas 24h ---
        const checkQuery = `
            SELECT facebook_lead_id, created_time 
            FROM leads 
            WHERE (email = $1 OR phone = $2) 
            AND created_time > $3 
            ORDER BY created_time DESC 
            LIMIT 1
        `;
        
        const oneDayAgo = Math.floor(Date.now() / 1000) - 86400; 
        const existingLead = await client.query(checkQuery, [email, phone, oneDayAgo]);

        if (existingLead.rows.length > 0) {
            webLeadId = existingLead.rows[0].facebook_lead_id;
            isNewLead = false;
            console.log(`⚠️ Lead Existente (Janela 24h). Mantendo ID: ${webLeadId}`);
        } else {
            webLeadId = data.custom_id || `WEB-${Date.now()}-${Math.floor(Math.random() * 1000)}`;
            console.log(`✨ Novo Lead Criado. ID: ${webLeadId}`);
        }

        const createdTime = isNewLead ? Math.floor(Date.now() / 1000) : existingLead.rows[0].created_time;

        const queryText = `
            INSERT INTO leads (facebook_lead_id, created_time, email, phone, first_name, last_name, fbc, fbp, client_ip_address, client_user_agent, platform, is_organic, form_name)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'site_tflow', false, 'Formulario Site')
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

        console.log('💾 [DB] Dados salvos/atualizados com sucesso!');
        res.status(200).json({ success: true, id: webLeadId });

    } catch (error) {
        console.error('❌ [ERRO] Falha ao salvar:', error);
        res.status(500).json({ success: false });
    } finally {
        client.release();
    }
});

// ============================================================================
// 3. ROTA: WEBHOOK (TRAVAS 2 e 3)
// ============================================================================
app.post('/webhook', async (req, res) => {
    console.log("--- 🔔 Webhook Recebido ---");
    try {
        const leadData = req.body;
        const crmEventName = leadData.tag ? leadData.tag.name : null;
        const facebookEventName = mapCRMEventToFacebookEvent(crmEventName);

        if (!facebookEventName) {
            console.log(`🚫 Ignorado: Evento de movimentação sem tag relevante (${crmEventName}).`);
            return res.status(200).send('Ignorado.');
        }

        if (!leadData.lead) return res.status(400).send('Sem dados.');
        
        const leadEmail = leadData.lead.email ? leadData.lead.email.toLowerCase().trim() : null;
        let leadPhone = leadData.lead.phone ? leadData.lead.phone.replace(/\D/g, '') : null;
        
        let crmFirstName = leadData.lead.first_name || '';
        let crmLastName = leadData.lead.last_name || '';
        if (!crmFirstName && leadData.lead.name) {
             const parts = leadData.lead.name.split(' ');
             crmFirstName = parts[0];
             crmLastName = parts.slice(1).join(' ');
        }

        let searchPhone = leadPhone;
        if (searchPhone && searchPhone.startsWith('55') && searchPhone.length > 11) {
            searchPhone = searchPhone.substring(2);
        }
        let phoneSuffix = '';
        if (leadPhone && leadPhone.length >= 8) {
            phoneSuffix = leadPhone.slice(-8); 
        }

        console.log(`🔍 [BUSCA] Iniciando varredura no DB...`);

        let dbRow;
        let result;
        let attempts = 0;
        
        const searchQuery = `
            SELECT * FROM leads 
            WHERE 
               (email IS NOT NULL AND email = $1)
               OR 
               (phone IS NOT NULL AND phone LIKE '%' || $2)
               OR
               (phone IS NOT NULL AND $3 <> '' AND phone LIKE '%' || $3)
            ORDER BY created_time ASC 
            LIMIT 1
        `;

        // Loop de Paciência (15 segundos)
        while (attempts < 5) {
            attempts++;
            result = await pool.query(searchQuery, [leadEmail, searchPhone || '0000', phoneSuffix]);
            
            if (result.rows.length > 0) {
                dbRow = result.rows[0];
                console.log(`✅ Lead encontrado no DB (Tentativa ${attempts})`);
                break; 
            } else {
                if (attempts < 5) {
                    console.log(`⏳ Lead ainda não chegou no Banco. Esperando... (${attempts}/5)`);
                    await sleep(3000);
                }
            }
        }

        // Resgate por Nome
        if (!dbRow && crmFirstName) {
            console.log(`⚠️ Tentando RESGATE POR NOME: "${crmFirstName}"...`);
            const nameSearchQuery = `
                SELECT * FROM leads 
                WHERE first_name ILIKE $1 
                AND (last_name ILIKE $2 OR $2 = '')
                AND created_time > $3
                ORDER BY created_time DESC 
                LIMIT 1
            `;
            const oneDayAgo = Math.floor(Date.now() / 1000) - 86400;
            const nameResult = await pool.query(nameSearchQuery, [crmFirstName, crmLastName, oneDayAgo]);
            
            if (nameResult.rows.length > 0) {
                dbRow = nameResult.rows[0];
                console.log(`✅ LEAD RESGATADO PELO NOME! ID: ${dbRow.facebook_lead_id}`);
            }
        }

        if (!dbRow) {
            console.log('❌ TIMEOUT: Lead não encontrado após todas as tentativas.');
            return res.status(200).send('Não encontrado.');
        }

        // ====================================================================
        // 🛑 TRAVA 2: ANTIGUIDADE (Lead Velho)
        // ====================================================================
        const isSiteLead = dbRow.facebook_lead_id && String(dbRow.facebook_lead_id).startsWith('WEB-');
        const now = Math.floor(Date.now() / 1000);
        const leadAgeSeconds = now - Number(dbRow.created_time);
        
        if (facebookEventName === 'Lead' && isSiteLead && leadAgeSeconds > 7200) {
            console.log(`🛑 [BLOQUEIO INTELIGENTE] Lead do Site Retornante.`);
            console.log(`   Motivo: Lead WEB criado há ${(leadAgeSeconds/3600).toFixed(1)} horas.`);
            return res.status(200).send('Bloqueado: Lead Antigo.');
        }

        // ====================================================================
        // 🛑 TRAVA 3: ESTADO (Duplicidade Imediata)
        // ====================================================================
        if (dbRow.last_sent_event === facebookEventName) {
            console.log(`🛑 [TRAVA DE ESTADO] O evento '${facebookEventName}' JÁ FOI ENVIADO para este lead.`);
            console.log(`   Ignorando solicitação duplicada do Webhook/CRM.`);
            return res.status(200).send('Duplicado: Já enviado.');
        }
        // ====================================================================

        const PIXEL_ID = process.env.PIXEL_ID;
        const FB_ACCESS_TOKEN = process.env.FB_ACCESS_TOKEN;
        if (!PIXEL_ID || !FB_ACCESS_TOKEN) return res.status(500).send('Erro Config.');

        const userData = {};
        const d = dbRow; 
        
        if (d.email) userData.em = [crypto.createHash('sha256').update(d.email).digest('hex')];
        if (d.phone) userData.ph = [crypto.createHash('sha256').update(d.phone).digest('hex')];
        if (d.first_name) userData.fn = [crypto.createHash('sha256').update(d.first_name.toLowerCase()).digest('hex')];
        if (d.last_name) userData.ln = [crypto.createHash('sha256').update(d.last_name.toLowerCase()).digest('hex')];
        if (d.city) userData.ct = [crypto.createHash('sha256').update(d.city.toLowerCase()).digest('hex')];
        if (d.estado) userData.st = [crypto.createHash('sha256').update(d.estado.toLowerCase()).digest('hex')];
        if (d.zip_code) userData.zp = [crypto.createHash('sha256').update(String(d.zip_code).replace(/\D/g, '')).digest('hex')];
        if (d.dob) userData.db = [crypto.createHash('sha256').update(String(d.dob).replace(/\D/g, '')).digest('hex')];

        if (d.fbc) userData.fbc = d.fbc;
        if (d.fbp) userData.fbp = d.fbp;
        if (d.client_ip_address) userData.client_ip_address = d.client_ip_address;
        if (d.client_user_agent) userData.client_user_agent = d.client_user_agent;

        if (d.facebook_lead_id) {
            userData.external_id = [crypto.createHash('sha256').update(d.facebook_lead_id).digest('hex')];
        }
        if (d.facebook_lead_id && !d.facebook_lead_id.startsWith('WEB-')) {
            userData.lead_id = d.facebook_lead_id;
        }

        const uniqueEventId = `${dbRow.facebook_lead_id}_${facebookEventName}`;
        const eventTime = Math.floor(Date.now() / 1000);
        
        // ====================================================================
        // CORREÇÃO: Definição dinâmica do Action Source (Website vs System)
        // ====================================================================
        let currentActionSource = 'system_generated'; 
        if (facebookEventName === 'Lead' || facebookEventName === 'CompleteRegistration') {
            currentActionSource = 'website';
        }
        // ====================================================================

        const eventData = { 
            event_name: facebookEventName, 
            event_time: eventTime,
            event_id: uniqueEventId, 
            action_source: currentActionSource, // AGORA É DINÂMICO
            user_data: userData,
            custom_data: { 
                event_source: 'crm',
                lead_event_source: 'Greenn Sales',
                campaign_name: dbRow.campaign_name,
                form_name: dbRow.form_name,
                lead_status: dbRow.lead_status
            }
        };

        const facebookAPIUrl = `https://graph.facebook.com/v24.0/${PIXEL_ID}/events?access_token=${FB_ACCESS_TOKEN}`;
        
        console.log(`📤 Enviando '${facebookEventName}' (ID: ${uniqueEventId}) como '${currentActionSource}'...`);
        await axios.post(facebookAPIUrl, { data: [eventData] });

        // === ATUALIZA O BANCO COM O NOVO STATUS (TRAVA 3) ===
        console.log(`📝 Atualizando status no DB para: ${facebookEventName}`);
        await pool.query(
            "UPDATE leads SET last_sent_event = $1 WHERE facebook_lead_id = $2",
            [facebookEventName, dbRow.facebook_lead_id]
        );
        // ====================================================

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
    const client = await pool.connect();
    try {
        const queryText = `SELECT * FROM leads ORDER BY created_time DESC`;
        const result = await client.query(queryText);
        
        if (result.rows.length === 0) return res.send('Banco vazio.');

        const mapColumns = {
            'facebook_lead_id': 'id',
            'created_time': 'created_time',
            'ad_id': 'ad_id', 'ad_name': 'ad_name',
            'adset_id': 'adset_id', 'adset_name': 'adset_name',
            'campaign_id': 'campaign_id', 'campaign_name': 'campaign_name',
            'form_id': 'form_id', 'form_name': 'form_name',
            'is_organic': 'is_organic', 'platform': 'platform',
            'first_name': 'nome', 'last_name': 'sobrenome',
            'phone': 'phone_number', 'email': 'email',
            'city': 'city', 'estado': 'state', 'zip_code': 'cep',
            'lead_status': 'lead_status',
            'last_sent_event': 'ultimo_evento_enviado' // Incluindo no backup
        };

        const dbKeys = Object.keys(mapColumns);
        const csvHeaders = Object.values(mapColumns);
        const csvRows = [];
        
        csvRows.push(csvHeaders.join(';')); 

        for (const row of result.rows) {
            const values = dbKeys.map(key => {
                let val = row[key];
                if (key === 'created_time' && val && !isNaN(val) && String(val).length > 5) {
                    try {
                        const dateObj = new Date(Number(val) * 1000); 
                        dateObj.setHours(dateObj.getHours() - 3);
                        val = dateObj.toISOString().replace('T', ' ').substring(0, 19);
                    } catch (e) { }
                }
                let escaped = ('' + (val || '')).replace(/"/g, '""');
                if (key === 'facebook_lead_id' || key === 'phone' || key === 'ad_id' || key === 'zip_code') {
                    return `="${escaped}"`; 
                }
                return `"${escaped}"`;
            });
            csvRows.push(values.join(';'));
        }

        res.setHeader('Content-Type', 'text/csv; charset=utf-8');
        res.setHeader('Content-Disposition', 'attachment; filename="backup_leads_v822.csv"');
        const csvContent = '\ufeff' + csvRows.join('\n');
        res.status(200).send(csvContent);

    } catch (error) {
        console.error('Erro CSV:', error);
        if (!res.headersSent) res.status(500).send('Erro ao gerar backup.');
    } finally {
        if (client) client.release();
    }
});

// ============================================================================
// 5. ROTA DE IMPORTAÇÃO
// ============================================================================
app.get('/importar', (req, res) => {
    res.send(`
        <!DOCTYPE html><html><head><title>Importar Leads V8.22</title><style>body{font-family:sans-serif;text-align:center;margin-top:50px}textarea{width:90%;max-width:1200px;height:400px;margin-top:20px}button{padding:10px 20px;font-size:16px;cursor:pointer}</style></head>
        <body><h1>Importar Leads (V8.22)</h1><p>Cole o JSON com colchetes: <b>[</b> { ... }, { ... } <b>]</b></p>
        <textarea id="leads-data" placeholder='[{"id": "...", "created_time": "12/15/25", ...}]'></textarea><br><button onclick="importLeads()">Importar</button><p id="status-message"></p>
        <script>
            async function importLeads(){
                const d=document.getElementById('leads-data').value;
                const s=document.getElementById('status-message');
                try{const r=await fetch('/import-leads',{method:'POST',headers:{'Content-Type':'application/json'},body:d});
                const t=await r.text();
                s.textContent=t;
                if(r.status === 201) s.style.color='green'; else s.style.color='red';
                }catch(e){s.textContent='Erro: '+e.message;s.style.color='red'}
            }
        </script></body></html>
    `);
});

app.post('/import-leads', async (req, res) => {
    const leadsToImport = req.body;
    if (!Array.isArray(leadsToImport)) return res.status(400).send('Erro: O JSON deve começar com [ e terminar com ].');

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
            
            let createdTimestamp = null;
            if (lead.created_time) {
                const asString = String(lead.created_time);
                if (asString.includes('/') || asString.includes('-')) {
                     const dateObj = new Date(lead.created_time);
                     if (!isNaN(dateObj.getTime())) createdTimestamp = Math.floor(dateObj.getTime() / 1000);
                } else {
                     createdTimestamp = (asString.length > 10) ? Math.floor(Number(asString) / 1000) : Number(asString);
                }
            }

            let isOrganic = false;
            if (lead.is_organic === true || String(lead.is_organic).toLowerCase() === 'true') {
                isOrganic = true;
            }

            const phoneRaw = lead.phone_number || lead.phone || '';
            
            await client.query(queryText, [
                id, createdTimestamp, lead.email, phoneRaw.replace(/\D/g, ''),
                lead.nome || lead.first_name, lead.sobrenome || lead.last_name, lead.data_de_nascimento || lead.dob, 
                lead.city, lead.state || lead.estado, lead.cep || lead.zip_code, 
                lead.ad_id, lead.ad_name, lead.adset_id, lead.adset_name, lead.campaign_id, lead.campaign_name, 
                lead.form_id, lead.form_name, lead.platform, isOrganic, lead.lead_status,
                lead.fbc, lead.fbp, lead.client_ip_address, lead.client_user_agent
            ]);
        }
        await client.query('COMMIT');
        res.status(201).send('Importação concluída com sucesso!');
    } catch (error) {
        await client.query('ROLLBACK');
        console.error('Erro Import:', error.message);
        res.status(500).send('ERRO TÉCNICO NO BANCO: ' + error.message);
    } finally {
        client.release();
    }
});

// ============================================================================
// 6. INICIALIZAÇÃO
// ============================================================================
app.get('/', (req, res) => res.send('🟢 Servidor V8.22 (Blindagem Tripla) Online!'));

const startServer = async () => {
    try {
        await initializeDatabase();
        app.listen(port, () => console.log(`🚀 Servidor na porta ${port}`));
    } catch (error) {
        console.error("❌ Falha:", error);
    }
};

startServer();
