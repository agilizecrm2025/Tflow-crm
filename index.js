// Importa as bibliotecas necessárias para o projeto
const express = require('express');
const { Client } = require('pg');
const axios = require('axios');
const crypto = require('crypto');

// Cria uma instância do Express e define a porta do servidor
const app = express();
const port = process.env.PORT || 3000;

// Middleware para entender dados JSON, com limite aumentado para 50mb
app.use(express.json({ limit: '50mb' }));

// Função para mapear o evento do CRM para o evento do Facebook
const mapCRMEventToFacebookEvent = (crmEvent) => {
    // Mapeia eventos do CRM para nomes de eventos do Facebook
    switch (crmEvent.toUpperCase()) {
        case 'NOVOS':
            return 'Lead';
        case 'ATENDEU':
            return 'Atendeu';
        case 'OPORTUNIDADE':
            return 'Oportunidade';
        case 'AVANÇADO':
            return 'Avançado';
        case 'VÍDEO':
            return 'Vídeo';
        case 'VENCEMOS':
            return 'Vencemos';
        case 'QUER EMPREGO':
            return 'Desqualificado';
        default:
            return crmEvent;
    }
};

// Conecta ao banco de dados e garante que a tabela e as colunas existam
const client = new Client({
    connectionString: process.env.DATABASE_URL,
    ssl: {
        rejectUnauthorized: false
    }
});

const initializeDatabase = async () => {
    try {
        await client.connect();
        console.log('Conexão com o banco de dados estabelecida.');

        // Garante que a tabela 'leads' com as colunas essenciais existe
        await client.query(`
            CREATE TABLE IF NOT EXISTS leads (
                facebook_lead_id TEXT PRIMARY KEY,
                phone TEXT,
                email TEXT
            );
        `);
        console.log('Tabela "leads" verificada/criada com sucesso.');

        // Verifica e adiciona a coluna 'full_name' se não existir
        const fullNameCheck = await client.query("SELECT 1 FROM information_schema.columns WHERE table_name='leads' AND column_name='full_name'");
        if (fullNameCheck.rows.length === 0) {
            await client.query("ALTER TABLE leads ADD COLUMN full_name TEXT;");
            console.log('Coluna "full_name" adicionada à tabela "leads".');
        }

        // Verifica e adiciona a coluna 'city' se não existir
        const cityCheck = await client.query("SELECT 1 FROM information_schema.columns WHERE table_name='leads' AND column_name='city'");
        if (cityCheck.rows.length === 0) {
            await client.query("ALTER TABLE leads ADD COLUMN city TEXT;");
            console.log('Coluna "city" adicionada à tabela "leads".');
        }

        // Verifica e adiciona a coluna 'estado' se não existir
        const estadoCheck = await client.query("SELECT 1 FROM information_schema.columns WHERE table_name='leads' AND column_name='estado'");
        if (estadoCheck.rows.length === 0) {
            await client.query("ALTER TABLE leads ADD COLUMN estado TEXT;");
            console.log('Coluna "estado" adicionada à tabela "leads".');
        }

    } catch (err) {
        console.error('Erro ao conectar ou inicializar o banco de dados', err.message);
    }
};

initializeDatabase();

// ENDPOINT: Rota para exibir o formulário de importação
app.get('/importar', (req, res) => {
    res.send(`
        <!DOCTYPE html>
        <html>
        <head>
            <title>Importar Leads</title>
            <style>
                body { font-family: sans-serif; text-align: center; margin-top: 50px; }
                textarea { width: 600px; height: 300px; margin-top: 20px; font-family: monospace; }
                button { padding: 10px 20px; font-size: 16px; cursor: pointer; }
                h1 { color: #333; }
                p { color: #666; }
            </style>
        </head>
        <body>
            <h1>Importar Leads para o Banco de Dados</h1>
            <p>Cole seus dados JSON aqui e clique em Importar. Lembre-se da ordem: ID Facebook, Telefone, E-mail, Nome Completo, Cidade, Estado.</p>
            <textarea id="leads-data" placeholder='[{"facebook_lead_id": "ID_FACEBOOK", "phone": "+5511987654321", "email": "email@exemplo.com", "full_name": "Nome Completo", "city": "Cidade", "estado": "Estado"}]'></textarea><br>
            <button onclick="importLeads()">Importar Leads</button>
            <p id="status-message" style="margin-top: 20px; font-weight: bold;"></p>

            <script>
                async function importLeads() {
                    const data = document.getElementById('leads-data').value;
                    const statusMessage = document.getElementById('status-message');
                    try {
                        const response = await fetch('/import-leads', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: data
                        });
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

// ENDPOINT: Onde o formulário de importação envia os dados
app.post('/import-leads', async (req, res) => {
    const leadsToImport = req.body;
    if (!Array.isArray(leadsToImport) || leadsToImport.length === 0) {
        return res.status(400).send('Dados de importação ausentes ou formato inválido.');
    }

    try {
        const queryText = `
            INSERT INTO leads (facebook_lead_id, phone, email, full_name, city, estado)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (facebook_lead_id) DO UPDATE SET
                phone = EXCLUDED.phone,
                email = EXCLUDED.email,
                full_name = EXCLUDED.full_name,
                city = EXCLUDED.city,
                estado = EXCLUDED.estado;
        `;

        for (const lead of leadsToImport) {
            if (!lead || typeof lead !== 'object' || !lead.facebook_lead_id) {
                console.error('Dados de lead inválidos no lote:', lead);
                continue;
            }

            const normalizedPhone = (lead.phone || '').replace(/\D/g, '');
            const leadFullName = lead.full_name || null;
            const leadCity = lead.city || null;
            const leadEstado = lead.estado || null;

            await client.query(queryText, [lead.facebook_lead_id, normalizedPhone, lead.email, leadFullName, leadCity, leadEstado]);
        }

        res.status(201).send('Leads importados com sucesso!');
    } catch (error) {
        console.error('Erro ao importar leads:', error.message);
        res.status(500).send('Erro interno do servidor.');
    }
});

// ENDPOINT DO WEBHOOK: Onde o CRM envia o evento
app.post('/webhook', async (req, res) => {
    try {
        const leadData = req.body;
        const crmEventName = leadData.tag ? leadData.tag.name : null;

        if (!crmEventName) {
            console.log('Webhook recebido, mas sem nome de evento válido. Nenhuma ação será tomada.');
            return res.status(200).send('Webhook recebido, mas sem nome de evento.');
        }

        const facebookEventName = mapCRMEventToFacebookEvent(crmEventName);

        if (!leadData || !leadData.lead) {
            return res.status(400).send('Dados do lead ausentes no webhook.');
        }

        const leadEmail = leadData.lead.email ? leadData.lead.email.toLowerCase() : null;
        const leadPhone = leadData.lead.phone ? leadData.lead.phone.replace(/\D/g, '') : null;

        if (!leadEmail && !leadPhone) {
            return res.status(400).send('E-mail ou telefone do lead ausentes no webhook.');
        }

        // Busca o ID do Facebook e os novos dados no banco de dados usando o e-mail ou telefone
        const result = await client.query(
            'SELECT facebook_lead_id, full_name, city, estado FROM leads WHERE email = $1 OR phone = $2',
            [leadEmail, leadPhone]
        );

        if (result.rows.length === 0) {
            console.log('ID do Facebook não encontrado para este lead no banco de dados.');
            return res.status(200).send('ID do Facebook não encontrado.');
        }

        const facebookLeadId = result.rows[0].facebook_lead_id;
        const leadFullName = result.rows[0].full_name;
        const leadCity = result.rows[0].city;
        const leadEstado = result.rows[0].estado;

        const PIXEL_ID = process.env.PIXEL_ID;
        const FB_ACCESS_TOKEN = process.env.FB_ACCESS_TOKEN;

        const userData = {};
        if (leadEmail) userData.em = [crypto.createHash('sha256').update(leadEmail).digest('hex')];
        if (leadPhone) userData.ph = [crypto.createHash('sha256').update(leadPhone).digest('hex')];
        if (leadFullName) userData.fn = [crypto.createHash('sha256').update(leadFullName.toLowerCase()).digest('hex')];
        if (leadCity) userData.ct = [crypto.createHash('sha256').update(leadCity.toLowerCase()).digest('hex')];
        if (leadEstado) userData.st = [crypto.createHash('sha256').update(leadEstado.toLowerCase()).digest('hex')];

        const eventData = {
            event_name: facebookEventName,
            event_time: Math.floor(Date.now() / 1000),
            action_source: 'system_generated',
            user_data: userData,
            custom_data: {
                lead_id: facebookLeadId,
                event_source: 'crm',
                lead_event_source: 'Your CRM'
            }
        };

        const facebookAPIUrl = `https://graph.facebook.com/v19.0/${PIXEL_ID}/events?access_token=${FB_ACCESS_TOKEN}`;

        await axios.post(facebookAPIUrl, {
            data: [eventData]
        });

        console.log(`Evento '${facebookEventName}' disparado com sucesso para o lead com ID: ${facebookLeadId}`);
        res.status(200).send('Evento de conversão enviado com sucesso!');

    } catch (error) {
        console.error('Erro ao processar o webhook:', error.message);
        res.status(500).send('Erro interno do servidor.');
    }
});

app.listen(port, () => {
    console.log(`Servidor rodando na porta ${port}`);
});
