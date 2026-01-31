// ============================================================================
// SERVIDOR DE INTELIGÊNCIA DE LEADS TFLOW (V8.38 - BI COMPLETO & TOP 15)
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
                client_user_agent TEXT,
                last_sent_event TEXT 
            );
        `;
        await client.query(createTableQuery);

        const colunasExtras = ['adset_name', 'campaign_name', 'dob', 'city', 'estado'];
        for (const col of colunasExtras) {
             const check = await client.query(`SELECT 1 FROM information_schema.columns WHERE table_name='leads' AND column_name='${col}'`);
             if (check.rows.length === 0) {
                 await client.query(`ALTER TABLE leads ADD COLUMN ${col} TEXT;`);
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
// 2. ROTA DE CAPTURA DO SITE
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
        
        const campaign = data.campaign_name || data.utm_campaign || data.campaign || null;
        const adset = data.adset_name || data.utm_content || data.adset || null;
        const dob = data.dob || data.data_nascimento || null;
        const city = data.city || data.cidade || null;
        const state = data.state || data.estado || data.uf || null;

        console.log(`🚀 [SITE TFLOW] RECEBIDO: ${firstName} | Campanha: ${campaign || 'N/A'}`);

        let webLeadId = null;
        let isNewLead = true;

        const checkQuery = `SELECT facebook_lead_id, created_time FROM leads WHERE (email = $1 OR phone = $2) AND created_time > $3 ORDER BY created_time DESC LIMIT 1`;
        const oneDayAgo = Math.floor(Date.now() / 1000) - 86400; 
        const existingLead = await client.query(checkQuery, [email, phone, oneDayAgo]);

        if (existingLead.rows.length > 0) {
            webLeadId = existingLead.rows[0].facebook_lead_id;
            isNewLead = false;
        } else {
            webLeadId = data.custom_id || `WEB-${Date.now()}-${Math.floor(Math.random() * 1000)}`;
        }

        const createdTime = isNewLead ? Math.floor(Date.now() / 1000) : existingLead.rows[0].created_time;

        const queryText = `
            INSERT INTO leads (
                facebook_lead_id, created_time, email, phone, first_name, last_name, 
                fbc, fbp, client_ip_address, client_user_agent, platform, is_organic, form_name,
                dob, city, estado, campaign_name, adset_name
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'site_tflow', false, 'Formulario Site', $11, $12, $13, $14, $15)
            ON CONFLICT (facebook_lead_id) DO UPDATE SET
                email = COALESCE(EXCLUDED.email, leads.email),
                phone = COALESCE(EXCLUDED.phone, leads.phone),
                first_name = COALESCE(EXCLUDED.first_name, leads.first_name),
                campaign_name = COALESCE(EXCLUDED.campaign_name, leads.campaign_name),
                adset_name = COALESCE(EXCLUDED.adset_name, leads.adset_name),
                dob = COALESCE(EXCLUDED.dob, leads.dob);
        `;

        await client.query(queryText, [
            webLeadId, createdTime, email, phone, firstName, lastName, 
            data.fbc, data.fbp, ip, userAgent,
            dob, city, state, campaign, adset
        ]);

        res.status(200).json({ success: true, id: webLeadId });

    } catch (error) {
        console.error('❌ [ERRO] Falha ao salvar:', error);
        res.status(500).json({ success: false });
    } finally {
        client.release();
    }
});

// ============================================================================
// 3. ROTA DE WEBHOOK
// ============================================================================
app.post('/webhook', async (req, res) => {
    console.log("--- 🔔 Webhook Recebido ---");
    try {
        const leadData = req.body;
        const crmEventName = leadData.tag ? leadData.tag.name : null;
        const facebookEventName = mapCRMEventToFacebookEvent(crmEventName);

        if (!facebookEventName) return res.status(200).send('Ignorado.');
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
        if (searchPhone && searchPhone.startsWith('55') && searchPhone.length > 11) searchPhone = searchPhone.substring(2);
        let phoneSuffix = (leadPhone && leadPhone.length >= 8) ? leadPhone.slice(-8) : '';

        let dbRow;
        let attempts = 0;
        
        while (attempts < 5) {
            attempts++;
            const result = await pool.query(`
                SELECT * FROM leads WHERE (email IS NOT NULL AND email = $1) OR (phone IS NOT NULL AND phone LIKE '%' || $2) OR (phone IS NOT NULL AND $3 <> '' AND phone LIKE '%' || $3) ORDER BY created_time ASC LIMIT 1
            `, [leadEmail, searchPhone || '0000', phoneSuffix]);
            
            if (result.rows.length > 0) {
                dbRow = result.rows[0];
                break; 
            } else {
                if (attempts < 5) await sleep(3000);
            }
        }

        if (!dbRow && crmFirstName) {
            const oneDayAgo = Math.floor(Date.now() / 1000) - 86400;
            const nameResult = await pool.query(`SELECT * FROM leads WHERE first_name ILIKE $1 AND (last_name ILIKE $2 OR $2 = '') AND created_time > $3 LIMIT 1`, [crmFirstName, crmLastName, oneDayAgo]);
            if (nameResult.rows.length > 0) dbRow = nameResult.rows[0];
        }

        if (!dbRow) return res.status(200).send('Não encontrado.');

        const isSiteLead = dbRow.facebook_lead_id && String(dbRow.facebook_lead_id).startsWith('WEB-');
        const now = Math.floor(Date.now() / 1000);
        if (facebookEventName === 'Lead' && isSiteLead && (now - Number(dbRow.created_time)) > 7200) {
            return res.status(200).send('Bloqueado: Lead Antigo.');
        }

        if (dbRow.last_sent_event === facebookEventName) {
            return res.status(200).send('Duplicado.');
        }

        const PIXEL_ID = process.env.PIXEL_ID;
        const FB_ACCESS_TOKEN = process.env.FB_ACCESS_TOKEN;
        const userData = {
            em: dbRow.email ? [crypto.createHash('sha256').update(dbRow.email).digest('hex')] : [],
            ph: dbRow.phone ? [crypto.createHash('sha256').update(dbRow.phone).digest('hex')] : []
        };
        if (dbRow.dob) {
            const cleanDob = String(dbRow.dob).replace(/\D/g, '');
            userData.db = [crypto.createHash('sha256').update(cleanDob).digest('hex')];
        }

        if (dbRow.fbc) userData.fbc = dbRow.fbc;
        if (dbRow.fbp) userData.fbp = dbRow.fbp;

        let currentActionSource = (facebookEventName === 'Lead' || facebookEventName === 'CompleteRegistration') ? 'website' : 'system_generated';

        await axios.post(`https://graph.facebook.com/v24.0/${PIXEL_ID}/events?access_token=${FB_ACCESS_TOKEN}`, {
            data: [{
                event_name: facebookEventName,
                event_time: Math.floor(Date.now() / 1000),
                event_id: `${dbRow.facebook_lead_id}_${facebookEventName}`,
                action_source: currentActionSource,
                user_data: userData
            }]
        });

        await pool.query("UPDATE leads SET last_sent_event = $1 WHERE facebook_lead_id = $2", [facebookEventName, dbRow.facebook_lead_id]);
        res.status(200).send('Enviado.');
    } catch (error) {
        console.error('❌ Erro Webhook:', error.message);
        res.status(500).send('Erro.');
    }
});

// ============================================================================
// 4. ROTA DE BACKUP
// ============================================================================
app.get('/baixar-backup', async (req, res) => {
    const client = await pool.connect();
    try {
        const queryText = `SELECT * FROM leads ORDER BY created_time DESC`;
        const result = await client.query(queryText);
        if (result.rows.length === 0) return res.send('Banco vazio.');
        
        let csv = 'id;created_time;name;email;phone;campaign;adset;status\n';
        result.rows.forEach(row => {
            let date = new Date(Number(row.created_time) * 1000).toISOString();
            csv += `${row.facebook_lead_id};${date};${row.first_name};${row.email};${row.phone};${row.campaign_name};${row.adset_name};${row.last_sent_event}\n`;
        });
        
        res.setHeader('Content-Type', 'text/csv');
        res.setHeader('Content-Disposition', 'attachment; filename="leads_tflow.csv"');
        res.status(200).send(csv);
    } finally { client.release(); }
});

// ============================================================================
// 5. ROTA DE IMPORTAÇÃO
// ============================================================================
app.get('/importar', (req, res) => {
     res.send(`<!DOCTYPE html><html><body><h1>Importar Leads TFlow</h1><p>Use Postman.</p></body></html>`);
});
app.post('/import-leads', async (req, res) => {
    const leadsToImport = req.body;
    if (!Array.isArray(leadsToImport)) return res.status(400).send('JSON Inválido');
    const client = await pool.connect();
    try {
        await client.query('BEGIN');
        const queryText = `
            INSERT INTO leads (facebook_lead_id, created_time, email, phone, first_name, last_name, dob, city, estado, zip_code, campaign_name, adset_name, platform, form_name, is_organic)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            ON CONFLICT (facebook_lead_id) DO NOTHING;
        `;
        for (const l of leadsToImport) {
             const id = l.id || l.facebook_lead_id;
             if(!id) continue;
             let time = l.created_time;
             if(String(time).includes('-')) time = Math.floor(new Date(time).getTime()/1000);
             
             await client.query(queryText, [
                 id, time, l.email, l.phone, l.first_name, l.last_name, l.dob, l.city, l.state, l.zip_code, l.campaign_name, l.adset_name, l.platform, l.form_name, false
             ]);
        }
        await client.query('COMMIT');
        res.status(201).send('Importado.');
    } catch(e) { 
        await client.query('ROLLBACK');
        res.status(500).send(e.message); 
    } finally { client.release(); }
});

// ============================================================================
// 6. DASHBOARD ANALÍTICO (V8.38 - BI TFLOW)
// ============================================================================

app.get('/dashboard', (req, res) => {
    const SENHA_MESTRA = 'tflow2026'; // <--- SENHA ATUALIZADA
    const senhaDigitada = req.query.senha;

    if (senhaDigitada !== SENHA_MESTRA) {
        return res.status(403).send('<h1 style="text-align:center;margin-top:50px">🔒 Acesso Negado</h1>');
    }

    res.send(`
<!DOCTYPE html>
<html lang="pt-br">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>BI TFlow</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdn.jsdelivr.net/npm/apexcharts"></script>
    <style>
        body { background-color: #0f172a; color: #e2e8f0; font-family: sans-serif; }
        .card { background-color: #1e293b; border-radius: 12px; padding: 20px; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.2); }
        .scroll-custom::-webkit-scrollbar { width: 6px; }
        .scroll-custom::-webkit-scrollbar-thumb { background-color: #475569; border-radius: 4px; }
    </style>
</head>
<body class="p-6">
    <div class="max-w-7xl mx-auto">
        
        <div class="flex flex-col md:flex-row justify-between items-center mb-8 gap-4">
            <div>
                <h1 class="text-3xl font-bold text-white tracking-tight">Monitoramento TFlow</h1>
                <p class="text-slate-400 text-sm mt-1">Inteligência de Tráfego e Vendas</p>
            </div>
            <div class="flex gap-2 bg-slate-800 p-1 rounded-lg overflow-x-auto">
                <button onclick="carregarDados('tres_dias')" class="px-3 py-2 bg-blue-600 rounded-lg text-sm hover:bg-blue-500 transition font-bold text-white shadow-lg whitespace-nowrap" id="btn-tres_dias">3D</button>
                <button onclick="carregarDados('semana')" class="px-3 py-2 bg-transparent rounded-lg text-sm hover:bg-slate-700 transition text-slate-300 whitespace-nowrap" id="btn-semana">7D</button>
                <button onclick="carregarDados('quinzena')" class="px-3 py-2 bg-transparent rounded-lg text-sm hover:bg-slate-700 transition text-slate-300 whitespace-nowrap" id="btn-quinzena">15D</button>
                <button onclick="carregarDados('trinta_dias')" class="px-3 py-2 bg-transparent rounded-lg text-sm hover:bg-slate-700 transition text-slate-300 whitespace-nowrap" id="btn-trinta_dias">30D</button>
                <button onclick="carregarDados('quarenta_cinco_dias')" class="px-3 py-2 bg-transparent rounded-lg text-sm hover:bg-slate-700 transition text-slate-300 whitespace-nowrap" id="btn-quarenta_cinco_dias">45D</button>
                <button onclick="carregarDados('noventa_dias')" class="px-3 py-2 bg-transparent rounded-lg text-sm hover:bg-slate-700 transition text-slate-300 whitespace-nowrap" id="btn-noventa_dias">90D</button>
            </div>
        </div>

        <div class="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
            <div class="card border-t-4 border-blue-500">
                <h3 class="text-slate-400 text-xs uppercase font-bold">Total Leads</h3>
                <p class="text-4xl font-bold text-white mt-2" id="kpi-total">0</p>
            </div>
            <div class="card border-t-4 border-indigo-500">
                <h3 class="text-slate-400 text-xs uppercase font-bold">Atendeu</h3>
                <p class="text-4xl font-bold text-white mt-2" id="kpi-atendeu">0</p>
            </div>
            <div class="card border-t-4 border-yellow-500">
                <h3 class="text-slate-400 text-xs uppercase font-bold">Oportunidade</h3>
                <p class="text-4xl font-bold text-white mt-2" id="kpi-oportunidade">0</p>
            </div>
            <div class="card border-t-4 border-green-500 bg-slate-800">
                <h3 class="text-slate-400 text-xs uppercase font-bold">Vendas</h3>
                <p class="text-4xl font-bold text-green-400 mt-2" id="kpi-vendas">0</p>
            </div>
        </div>

        <div class="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
            <div class="card">
                <h2 class="text-lg font-semibold mb-2 text-white">Funil Geral (Etapas)</h2>
                <div id="chart-funnel"></div>
            </div>
            <div class="card">
                <h2 class="text-lg font-semibold mb-2 text-white">Top 7 Campanhas (Leads vs Vendas)</h2>
                <div id="chart-compare"></div>
            </div>
        </div>
        
        <div class="grid grid-cols-1 gap-6 mb-8">
             <div class="card">
                <h2 class="text-lg font-semibold mb-2 text-white">Top 15 Estados (UF)</h2>
                <div id="chart-states"></div>
            </div>
        </div>

        <div class="card mb-8">
            <h2 class="text-xl font-bold text-white mb-4 border-b border-slate-700 pb-3">🚀 Matriz de Performance (Funil por Conjunto)</h2>
            <div class="overflow-x-auto scroll-custom max-h-96">
                <table class="w-full text-sm text-left text-slate-300">
                    <thead class="text-xs text-slate-400 uppercase bg-slate-800 sticky top-0">
                        <tr>
                            <th class="px-4 py-3">Campanha</th>
                            <th class="px-4 py-3">Conjunto (Adset)</th>
                            <th class="px-4 py-3 text-center font-bold text-blue-400">Leads</th>
                            <th class="px-4 py-3 text-center">Atendeu</th>
                            <th class="px-4 py-3 text-center">Oportun.</th>
                            <th class="px-4 py-3 text-center">Avançado</th>
                            <th class="px-4 py-3 text-center">Vídeo</th>
                            <th class="px-4 py-3 text-center font-bold text-green-400">Vendas</th>
                            <th class="px-4 py-3 text-center">Conv. %</th>
                        </tr>
                    </thead>
                    <tbody id="table-matrix"></tbody>
                </table>
            </div>
        </div>

        <div class="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
            
            <div class="card">
                <h2 class="text-lg font-semibold text-white mb-4">🏆 Faixa Etária vs. Qualidade</h2>
                <div class="overflow-x-auto scroll-custom max-h-80">
                    <table class="w-full text-sm text-left text-slate-300">
                        <thead class="text-xs text-slate-400 uppercase bg-slate-800 sticky top-0">
                            <tr>
                                <th class="px-4 py-3">Faixa Etária</th>
                                <th class="px-4 py-3 text-center text-blue-400">Leads</th>
                                <th class="px-4 py-3 text-center">Atendeu</th>
                                <th class="px-4 py-3 text-center">Oport.</th>
                                <th class="px-4 py-3 text-center">Avanç.</th>
                                <th class="px-4 py-3 text-center">Vídeo</th>
                                <th class="px-4 py-3 text-center text-green-400">Vendas</th>
                            </tr>
                        </thead>
                        <tbody id="table-age"></tbody>
                    </table>
                </div>
                <p class="text-xs text-slate-500 mt-2 text-center">Apenas 20 a 65 anos</p>
            </div>

            <div class="card">
                <h2 class="text-lg font-semibold text-white mb-4">🗺️ Estados vs. Qualidade (Todos)</h2>
                <div class="overflow-x-auto scroll-custom max-h-80">
                    <table class="w-full text-sm text-left text-slate-300">
                        <thead class="text-xs text-slate-400 uppercase bg-slate-800 sticky top-0">
                            <tr>
                                <th class="px-4 py-3">Estado</th>
                                <th class="px-4 py-3 text-center text-blue-400">Leads</th>
                                <th class="px-4 py-3 text-center">Atendeu</th>
                                <th class="px-4 py-3 text-center">Oport.</th>
                                <th class="px-4 py-3 text-center">Avanç.</th>
                                <th class="px-4 py-3 text-center">Vídeo</th>
                                <th class="px-4 py-3 text-center text-green-400">Vendas</th>
                            </tr>
                        </thead>
                        <tbody id="table-states"></tbody>
                    </table>
                </div>
            </div>

        </div>

        <div class="card">
            <h2 class="text-lg font-semibold mb-4 text-white">📋 Últimos 50 Leads (Conferência)</h2>
            <div class="overflow-x-auto scroll-custom max-h-80">
                <table class="w-full text-sm text-left text-slate-300">
                    <thead class="text-xs text-slate-400 uppercase bg-slate-800 sticky top-0">
                        <tr>
                            <th class="px-4 py-3">Data</th>
                            <th class="px-4 py-3">Nome</th>
                            <th class="px-4 py-3">Nascimento</th>
                            <th class="px-4 py-3">Campanha</th>
                            <th class="px-4 py-3">Status</th>
                        </tr>
                    </thead>
                    <tbody id="table-leads"></tbody>
                </table>
            </div>
        </div>

    </div>

    <script>
        Apex.grid = { borderColor: '#334155' };
        Apex.chart = { background: 'transparent', toolbar: { show: false } };
        Apex.tooltip = { theme: 'dark' };

        let chartFunnelObj = null;
        let chartCompareObj = null;
        let chartStatesObj = null;

        function formatarDataNasc(raw) {
            if (!raw) return '--';
            const s = String(raw).replace(/\D/g, ''); 
            if (s.length === 8) {
                const y = s.substring(0, 4);
                const m = s.substring(4, 6);
                const d = s.substring(6, 8);
                return \`\${d}/\${m}/\${y}\`;
            }
            return raw; 
        }

        async function carregarDados(periodo) {
            ['tres_dias', 'semana', 'quinzena', 'trinta_dias', 'quarenta_cinco_dias', 'noventa_dias'].forEach(p => {
                const btn = document.getElementById('btn-' + p);
                if (btn) {
                    if(p === periodo) {
                        btn.classList.remove('bg-transparent', 'text-slate-300');
                        btn.classList.add('bg-blue-600', 'text-white', 'shadow-lg');
                    } else {
                        btn.classList.add('bg-transparent', 'text-slate-300');
                        btn.classList.remove('bg-blue-600', 'text-white', 'shadow-lg');
                    }
                }
            });

            try {
                const res = await fetch('/api/kpis?periodo=' + periodo);
                const data = await res.json();
                atualizarInterface(data);
            } catch (e) { console.error('Erro:', e); }
        }

        function atualizarInterface(data) {
            document.getElementById('kpi-total').innerText = data.total;
            document.getElementById('kpi-atendeu').innerText = data.funil.atendeu;
            document.getElementById('kpi-oportunidade').innerText = data.funil.oportunidade;
            document.getElementById('kpi-vendas').innerText = data.funil.vencemos;
            
            // 1. Gráfico Funil
            const categoriesFunnel = ['Novos', 'Atendeu', 'Oportunidade', 'Avançado', 'Vídeo', 'Vencemos'];
            const seriesFunnel = [data.total, data.funil.atendeu, data.funil.oportunidade, data.funil.avancado, data.funil.video, data.funil.vencemos];

            if (chartFunnelObj) chartFunnelObj.destroy();
            chartFunnelObj = new ApexCharts(document.querySelector("#chart-funnel"), {
                series: [{ name: 'Leads', data: seriesFunnel }],
                chart: { type: 'bar', height: 300 },
                plotOptions: { bar: { borderRadius: 4, horizontal: true, distributed: true } },
                colors: ['#64748b', '#3b82f6', '#f59e0b', '#8b5cf6', '#ec4899', '#10b981'],
                xaxis: { categories: categoriesFunnel, labels: { style: { colors: '#cbd5e1' } } },
                yaxis: { labels: { style: { colors: '#cbd5e1' } } },
                legend: { show: false }
            });
            chartFunnelObj.render();

            // 2. Gráfico Comparativo
            const campNames = data.topCampanhas.map(c => c.nome.substring(0, 15));
            const campLeads = data.topCampanhas.map(c => c.leads);
            const campVendas = data.topCampanhas.map(c => c.vendas);

            if (chartCompareObj) chartCompareObj.destroy();
            chartCompareObj = new ApexCharts(document.querySelector("#chart-compare"), {
                series: [{ name: 'Leads', data: campLeads }, { name: 'Vendas', data: campVendas }],
                chart: { type: 'bar', height: 300 },
                colors: ['#3b82f6', '#10b981'],
                xaxis: { categories: campNames, labels: { style: { colors: '#cbd5e1' } } },
                yaxis: { labels: { style: { colors: '#cbd5e1' } } },
                legend: { labels: { colors: '#cbd5e1' } },
                plotOptions: { bar: { borderRadius: 4, columnWidth: '50%' } }
            });
            chartCompareObj.render();

            // 3. Gráfico Estados (TOP 15) - Altura Ajustada
            const stateNames = data.topEstados.map(e => e.nome);
            const stateVals = data.topEstados.map(e => e.qtd);

            if (chartStatesObj) chartStatesObj.destroy();
            chartStatesObj = new ApexCharts(document.querySelector("#chart-states"), {
                series: [{ name: 'Leads', data: stateVals }],
                chart: { type: 'bar', height: 320 }, 
                colors: ['#8b5cf6'],
                xaxis: { categories: stateNames, labels: { style: { colors: '#cbd5e1' } } },
                yaxis: { labels: { style: { colors: '#cbd5e1' } } },
                legend: { show: false },
                plotOptions: { bar: { borderRadius: 4, columnWidth: '40%' } }
            });
            chartStatesObj.render();

            // 4. Tabela Matriz
            const tbodyMatrix = document.getElementById('table-matrix');
            tbodyMatrix.innerHTML = data.matrix.map(row => {
                const conv = row.leads > 0 ? ((row.vendas / row.leads) * 100).toFixed(1) + '%' : '0.0%';
                return \`<tr class="border-b border-slate-700 hover:bg-slate-700/50">
                    <td class="px-4 py-3 font-medium text-white">\${row.campaign}</td>
                    <td class="px-4 py-3 text-slate-400">\${row.adset}</td>
                    <td class="px-4 py-3 text-center text-blue-400 font-bold">\${row.leads}</td>
                    <td class="px-4 py-3 text-center">\${row.atendeu}</td>
                    <td class="px-4 py-3 text-center">\${row.oportunidade}</td>
                    <td class="px-4 py-3 text-center">\${row.avancado}</td>
                    <td class="px-4 py-3 text-center">\${row.video}</td>
                    <td class="px-4 py-3 text-center text-green-400 font-bold">\${row.vendas}</td>
                    <td class="px-4 py-3 text-center text-xs">\${conv}</td>
                </tr>\`;
            }).join('');

            // 5. Tabela Idade Cruzada
            const tbodyAge = document.getElementById('table-age');
            tbodyAge.innerHTML = data.ageData.map(row => {
                return \`<tr class="border-b border-slate-700 hover:bg-slate-700/50">
                    <td class="px-4 py-3 font-medium text-white">\${row.range} anos</td>
                    <td class="px-4 py-3 text-center text-blue-400 font-bold">\${row.leads}</td>
                    <td class="px-4 py-3 text-center">\${row.atendeu}</td>
                    <td class="px-4 py-3 text-center">\${row.oportunidade}</td>
                    <td class="px-4 py-3 text-center">\${row.avancado}</td>
                    <td class="px-4 py-3 text-center">\${row.video}</td>
                    <td class="px-4 py-3 text-center text-green-400 font-bold">\${row.vencemos}</td>
                </tr>\`;
            }).join('');

            // 6. Tabela Estados Cruzada (SEM FILTRO)
            const tbodyStates = document.getElementById('table-states');
            if(data.stateData.length === 0) {
                tbodyStates.innerHTML = '<tr><td colspan="6" class="text-center py-4 text-slate-500">Nenhum estado.</td></tr>';
            } else {
                tbodyStates.innerHTML = data.stateData.map(row => {
                    return \`<tr class="border-b border-slate-700 hover:bg-slate-700/50">
                        <td class="px-4 py-3 font-medium text-white">\${row.state}</td>
                        <td class="px-4 py-3 text-center text-blue-400 font-bold">\${row.leads}</td>
                        <td class="px-4 py-3 text-center">\${row.atendeu}</td>
                        <td class="px-4 py-3 text-center">\${row.oportunidade}</td>
                        <td class="px-4 py-3 text-center">\${row.avancado}</td>
                        <td class="px-4 py-3 text-center">\${row.video}</td>
                        <td class="px-4 py-3 text-center text-green-400 font-bold">\${row.vencemos}</td>
                    </tr>\`;
                }).join('');
            }

            // 7. Tabela Leads Recentes
            const tbodyLeads = document.getElementById('table-leads');
            tbodyLeads.innerHTML = data.recentLeads.map(l => {
                const date = new Date(Number(l.created_time) * 1000).toLocaleDateString('pt-BR');
                const dobFormatado = formatarDataNasc(l.dob);
                return \`<tr class="border-b border-slate-700 hover:bg-slate-700/50">
                    <td class="px-4 py-3 text-slate-400 text-xs">\${date}</td>
                    <td class="px-4 py-3 text-white">\${l.first_name}</td>
                    <td class="px-4 py-3 text-yellow-400">\${dobFormatado}</td>
                    <td class="px-4 py-3 text-xs text-slate-400">\${l.campaign_name || '-'}</td>
                    <td class="px-4 py-3 text-xs">\${l.last_sent_event || 'Novo'}</td>
                </tr>\`;
            }).join('');
        }

        carregarDados('tres_dias');
    </script>
</body>
</html>
    `);
});

app.get('/api/kpis', async (req, res) => {
    const { periodo } = req.query; 
    const client = await pool.connect();
    try {
        const now = new Date();
        now.setHours(now.getHours() - 3);
        
        let daysToSubtract = 3;
        if (periodo === 'semana') daysToSubtract = 7;
        else if (periodo === 'quinzena') daysToSubtract = 15;
        else if (periodo === 'trinta_dias') daysToSubtract = 30;
        else if (periodo === 'quarenta_cinco_dias') daysToSubtract = 45;
        else if (periodo === 'noventa_dias') daysToSubtract = 90;

        const startDate = new Date(now);
        startDate.setDate(startDate.getDate() - daysToSubtract);
        startDate.setHours(0,0,0,0);
        const startTimestamp = Math.floor(startDate.getTime() / 1000);

        const queryText = `
            SELECT facebook_lead_id, last_sent_event, campaign_name, adset_name, dob, estado, first_name, created_time
            FROM leads 
            WHERE created_time >= $1
            ORDER BY created_time DESC
        `;
        const result = await client.query(queryText, [startTimestamp]);
        
        const stats = {
            total: 0,
            funil: { atendeu: 0, oportunidade: 0, avancado: 0, video: 0, vencemos: 0 },
            matrix: [], 
            topCampanhas: [], 
            recentLeads: result.rows.slice(0, 50),
            ageData: [],
            stateData: []
        };

        const matrixMap = {}; 
        const ageMap = {};
        const stateMap = {};

        // INICIALIZA BUCKETS DE IDADE (AGORA COM 'atendeu' e zerados)
        const buckets = ['20-24', '25-29', '30-39', '40-49', '50-59', '60-65'];
        buckets.forEach(b => {
            ageMap[b] = { range: b, leads: 0, atendeu: 0, oportunidade: 0, avancado: 0, video: 0, vencemos: 0 };
        });

        result.rows.forEach(row => {
            stats.total++;
            
            const st = row.last_sent_event ? row.last_sent_event.toUpperCase() : '';
            
            // FUNIL CUMULATIVO GERAL
            // Se está em 'Vencemos', ele também passou por todas as anteriores
            const isVenda = (st === 'VENCEMOS' || st === 'VENDA');
            const isVideo = (st === 'VÍDEO' || st === 'VIDEO') || isVenda;
            const isAvancado = (st === 'AVANÇADO') || isVideo;
            const isOportunidade = (st === 'OPORTUNIDADE') || isAvancado;
            const isAtendeu = (st === 'ATENDEU') || isOportunidade;

            if (isAtendeu) stats.funil.atendeu++;
            if (isOportunidade) stats.funil.oportunidade++;
            if (isAvancado) stats.funil.avancado++;
            if (isVideo) stats.funil.video++;
            if (isVenda) stats.funil.vencemos++;

            // MATRIZ (Campanha/Adset)
            const camp = row.campaign_name;
            const adset = row.adset_name || 'Geral';
            if (camp && camp.toUpperCase() !== 'SEM CAMPANHA') {
                const key = `${camp}|${adset}`;
                if (!matrixMap[key]) matrixMap[key] = { campaign: camp, adset: adset, leads: 0, atendeu: 0, oportunidade: 0, avancado: 0, video: 0, vendas: 0 };
                matrixMap[key].leads++;
                if (isAtendeu) matrixMap[key].atendeu++;
                if (isOportunidade) matrixMap[key].oportunidade++;
                if (isAvancado) matrixMap[key].avancado++;
                if (isVideo) matrixMap[key].video++;
                if (isVenda) matrixMap[key].vendas++;
            }

            // ESTADOS (TODOS) - AGORA COM 'atendeu' INICIALIZADO CORRETAMENTE
            const uf = row.estado ? row.estado.toUpperCase() : null;
            if (uf && uf.length === 2) {
                if (!stateMap[uf]) stateMap[uf] = { state: uf, leads: 0, atendeu: 0, oportunidade: 0, avancado: 0, video: 0, vencemos: 0 };
                stateMap[uf].leads++;
                if (isAtendeu) stateMap[uf].atendeu++;
                if (isOportunidade) stateMap[uf].oportunidade++;
                if (isAvancado) stateMap[uf].avancado++;
                if (isVideo) stateMap[uf].video++;
                if (isVenda) stateMap[uf].vencemos++;
            }

            // IDADES (TODOS)
            if (row.dob) {
                let anoNasc = 0;
                let dobStr = String(row.dob).replace(/\D/g, ''); 
                if (row.dob.includes('-')) anoNasc = parseInt(row.dob.split('-')[0]);
                else if (row.dob.includes('/')) anoNasc = parseInt(row.dob.split('/')[2]);
                else if (dobStr.length === 8) anoNasc = parseInt(dobStr.substring(0, 4));
                else if (dobStr.length === 4) anoNasc = parseInt(dobStr);

                if (anoNasc > 1900 && anoNasc < new Date().getFullYear()) {
                    const idade = new Date().getFullYear() - anoNasc;
                    if (idade >= 20 && idade <= 65) {
                        let b = '';
                        if (idade <= 24) b = '20-24';
                        else if (idade <= 29) b = '25-29';
                        else if (idade <= 39) b = '30-39';
                        else if (idade <= 49) b = '40-49';
                        else if (idade <= 59) b = '50-59';
                        else b = '60-65';

                        ageMap[b].leads++;
                        if (isAtendeu) ageMap[b].atendeu++;
                        if (isOportunidade) ageMap[b].oportunidade++;
                        if (isAvancado) ageMap[b].avancado++;
                        if (isVideo) ageMap[b].video++;
                        if (isVenda) ageMap[b].vencemos++;
                    }
                }
            }
        });

        // FINALIZA RANKINGS
        stats.matrix = Object.values(matrixMap).sort((a, b) => b.leads - a.leads);
        
        const campAgg = {};
        stats.matrix.forEach(m => {
            if (!campAgg[m.campaign]) campAgg[m.campaign] = { nome: m.campaign, leads: 0, vendas: 0 };
            campAgg[m.campaign].leads += m.leads;
            campAgg[m.campaign].vendas += m.vendas;
        });
        stats.topCampanhas = Object.values(campAgg).sort((a, b) => b.leads - a.leads).slice(0, 7);

        stats.topEstados = Object.entries(stateMap)
            .map(([nome, obj]) => ({ nome, qtd: obj.leads }))
            .sort((a, b) => b.qtd - a.qtd)
            .slice(0, 15); // AQUI: Alterado para TOP 15

        // Tabela Estados (TODOS)
        stats.stateData = Object.values(stateMap)
            .sort((a, b) => b.leads - a.leads);

        // Processa Idades (Remove vazios)
        stats.ageData = Object.values(ageMap).filter(a => a.leads > 0);

        res.json(stats);

    } catch (error) {
        console.error(error);
        res.status(500).json({ error: 'Erro ao buscar KPIs' });
    } finally {
        client.release();
    }
});

// ============================================================================
// 7. INICIALIZAÇÃO
// ============================================================================
app.get('/', (req, res) => res.send('🟢 Servidor V8.38 TFLOW (Top 15 Estados) Online!'));

const startServer = async () => {
    try {
        await initializeDatabase();
        app.listen(port, () => console.log(`🚀 Servidor na porta ${port}`));
    } catch (error) {
        console.error("❌ Falha:", error);
    }
};

startServer();
