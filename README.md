# Email Enrichment Workflow Service

A comprehensive email enrichment service that fetches leads from Supabase, enriches them with validated email addresses using Hunter.io and Perplexity APIs, and updates the database with verified contacts.

## Workflow Overview

### 1. Source
- **Fetch leads** from Supabase `leads` table
- Process leads with company domains but missing validated emails

### 2. Primary Email Enrichment (Hunter.io)
- **Domain Search API**: For each lead domain, query Hunter.io Domain Search API
- **Extract valid emails**: Keep emails with confidence scores > 70%
- **Filter quality**: Prioritize role-based emails (CEO, CTO, Founder, etc.) over generic ones

### 3. Fallback Enrichment (Perplexity API)
When Hunter.io returns no valid emails or only generic emails (support@, info@, etc.):
- **Query Perplexity API**: "Find the owner/founder/decision-maker name for {company_domain}"
- **Parse response**: Extract full names from the AI-generated response
- **Generate permutations**:
  - firstname.lastname@domain
  - f.lastname@domain
  - firstname@domain
  - lastname@domain
  - firstname.l@domain
  - flastname@domain

### 4. Email Validation (Hunter.io)
- **Email Verifier API**: Run each generated email through Hunter.io verification
- **Validation criteria**:
  - `deliverable`: Email exists and accepts mail
  - `valid`: Email syntax is correct
  - `accept_all`: Avoid domains that accept all emails
- **Confidence threshold**: Only keep emails with score > 80

### 5. Post-processing
- **Update Supabase**: Save validated emails to `emails` table with:
  - `lead_id`: Reference to original lead
  - `email`: Validated email address
  - `source`: "hunter_direct" | "perplexity_generated"
  - `confidence_score`: Hunter.io validation score
  - `email_type`: "primary" | "secondary"
  - `verified_at`: Timestamp
  - `status`: "active" | "inactive"

## 📋 Prerequisites

### APIs Required
1. **Supabase** - Database and API management
2. **Hunter.io** - Email finding and verification
   - Domain Search API
   - Email Verifier API
3. **Perplexity AI** - Fallback name discovery

### Environment Setup
```bash
# Required environment variables
SUPABASE_URL=your_supabase_url
SUPABASE_ANON_KEY=your_supabase_anon_key
SUPABASE_SERVICE_ROLE_KEY=your_service_role_key
HUNTER_IO_API_KEY=your_hunter_api_key
PERPLEXITY_API_KEY=your_perplexity_api_key
```

## 🗄️ Database Schema

### Leads Table
```sql
CREATE TABLE leads (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_name TEXT NOT NULL,
    domain TEXT NOT NULL,
    industry TEXT,
    company_size TEXT,
    location TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

### Emails Table
```sql
CREATE TABLE emails (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    lead_id UUID REFERENCES leads(id) ON DELETE CASCADE,
    email TEXT NOT NULL,
    source TEXT NOT NULL CHECK (source IN ('hunter_direct', 'perplexity_generated')),
    confidence_score INTEGER CHECK (confidence_score >= 0 AND confidence_score <= 100),
    email_type TEXT NOT NULL CHECK (email_type IN ('primary', 'secondary')),
    hunter_status TEXT CHECK (hunter_status IN ('deliverable', 'undeliverable', 'risky', 'unknown')),
    verified_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'inactive')),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    UNIQUE(lead_id, email)
);

-- Indexes for performance
CREATE INDEX idx_emails_lead_id ON emails(lead_id);
CREATE INDEX idx_emails_status ON emails(status);
CREATE INDEX idx_emails_confidence ON emails(confidence_score DESC);
```

### Jobs Table (for job queuing and batching)
```sql
CREATE TABLE jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id TEXT NOT NULL UNIQUE,
    lead_ids JSONB,
    domains JSONB,
    process_all BOOLEAN DEFAULT FALSE,
    skip_existing BOOLEAN DEFAULT TRUE,
    max_concurrent INTEGER,
    batch_size INTEGER,
    status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'paused', 'completed', 'failed', 'cancelled')),
    progress JSONB DEFAULT '{}',
    error_message TEXT,
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    total_leads INTEGER DEFAULT 0,
    processed_leads INTEGER DEFAULT 0,
    failed_leads INTEGER DEFAULT 0,
    created_emails INTEGER DEFAULT 0,
    api_calls_hunter INTEGER DEFAULT 0,
    api_calls_perplexity INTEGER DEFAULT 0,
    processing_time_seconds FLOAT DEFAULT 0.0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX idx_jobs_status ON jobs(status);
CREATE INDEX idx_jobs_created_at ON jobs(created_at DESC);
CREATE INDEX idx_jobs_job_id ON jobs(job_id);
```

## 🚀 Setup Instructions

### Option 1: Local Development (Windows/macOS/Linux)

#### 1. Clone and Setup
```bash
git clone <repository-url>
cd email-enrichment-service
```

#### 2. Setup Environment (Windows PowerShell)
```powershell
# Run setup script
.\scripts\run.ps1 setup

# Or manually:
python -m venv venv
venv\Scripts\activate  # On Windows
pip install -r requirements.txt
```

#### 3. Configure Environment Variables
```bash
# Copy example environment file
cp env.example .env

# Edit .env with your API keys and database credentials
# Required: SUPABASE_URL, SUPABASE_ANON_KEY, SUPABASE_SERVICE_ROLE_KEY
# Required: HUNTER_IO_API_KEY, PERPLEXITY_API_KEY
```

#### 4. Run the Service
```powershell
# Start worker
.\scripts\run.ps1 worker

# Start health check server
.\scripts\run.ps1 health

# Run CLI commands
.\scripts\run.ps1 cli process-all
```

### Option 2: Render Deployment (Recommended)

#### 1. Connect to Render
1. Go to [Render.com](https://render.com) and sign up/login
2. Connect your GitHub repository
3. Select "Blueprint" when creating a new service
4. Choose the `render.yaml` file from your repository

#### 2. Configure Environment Variables in Render Dashboard
Set these environment variables in your Render service:

**Required Database (Supabase):**
```
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_ANON_KEY=your-anon-key
SUPABASE_SERVICE_ROLE_KEY=your-service-role-key
```

**Required API Keys:**
```
HUNTER_IO_API_KEY=your-hunter-api-key
PERPLEXITY_API_KEY=your-perplexity-api-key
```

**Optional Configuration:**
```
LOG_LEVEL=INFO
MAX_WORKERS=3
BATCH_SIZE=10
REQUEST_TIMEOUT=30
REDIS_URL=redis://your-redis-url  # If using Redis caching
```

#### 3. Deploy
- Render will automatically build and deploy using the `render.yaml` configuration
- The worker service will start processing emails automatically
- The health check service provides monitoring endpoints

#### 4. Monitor Your Service
- View logs in the Render dashboard
- Check health at: `https://your-service.onrender.com/health`
- View stats at: `https://your-service.onrender.com/stats`

### Option 3: Docker Deployment

#### 1. Build and Run Locally
```bash
# Build Docker image
docker build -t email-enrichment .

# Run with environment variables
docker run --env-file .env email-enrichment
```

#### 2. Docker Compose (Optional)
```yaml
# docker-compose.yml
version: '3.8'
services:
  email-enrichment:
    build: .
    env_file: .env
    restart: unless-stopped
```

### 2. Environment Configuration
Create a `.env` file in the project root:
```env
# Supabase Configuration
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_ANON_KEY=your-anon-key
SUPABASE_SERVICE_ROLE_KEY=your-service-role-key

# Hunter.io API
HUNTER_IO_API_KEY=your-hunter-api-key

# Perplexity AI API
PERPLEXITY_API_KEY=your-perplexity-api-key

# Service Configuration
LOG_LEVEL=INFO
MAX_WORKERS=5
BATCH_SIZE=10
REQUEST_TIMEOUT=30
```

### 3. API Keys Setup

#### Hunter.io
1. Sign up at [hunter.io](https://hunter.io)
2. Get your API key from the dashboard
3. Verify API limits (free tier: 50 searches/month)

#### Perplexity AI
1. Sign up at [perplexity.ai](https://www.perplexity.ai/)
2. Generate API key from settings
3. Note rate limits and costs

### 4. Database Setup
Run the SQL schema provided above in your Supabase SQL editor.

## 📁 Project Structure

```
email-enrichment-service/
├── src/
│   ├── __init__.py
│   ├── main.py                 # Main service entry point
│   ├── config.py               # Configuration management
│   ├── database.py             # Supabase client and operations
│   ├── hunter_client.py        # Hunter.io API client
│   ├── perplexity_client.py    # Perplexity API client
│   ├── email_generator.py      # Email permutation generator
│   ├── validator.py            # Email validation logic
│   └── models.py               # Pydantic models
├── tests/
│   ├── __init__.py
│   ├── test_hunter_client.py
│   ├── test_perplexity_client.py
│   └── test_email_generator.py
├── requirements.txt
├── .env.example
├── README.md
└── Dockerfile
```

## 🔧 Implementation Details

### Core Components

#### 1. Lead Processor (`main.py`)
- Orchestrates the entire enrichment workflow
- Handles batch processing with configurable workers
- Implements retry logic and error handling

#### 2. Hunter.io Client (`hunter_client.py`)
```python
class HunterClient:
    def domain_search(self, domain: str) -> List[EmailResult]
    def verify_email(self, email: str) -> VerificationResult
```

#### 3. Perplexity Client (`perplexity_client.py`)
```python
class PerplexityClient:
    def find_person_name(self, company_domain: str) -> str
```

#### 4. Email Generator (`email_generator.py`)
```python
def generate_email_permutations(first_name: str, last_name: str, domain: str) -> List[str]:
    # Generates various email format combinations
```

### Error Handling
- **Rate Limiting**: Implements exponential backoff for API calls
- **API Failures**: Graceful fallback between enrichment methods
- **Database Errors**: Transaction rollback and retry mechanisms
- **Invalid Data**: Comprehensive validation and logging

### Performance Optimizations
- **Concurrent Processing**: Async/await with semaphore limiting
- **Batch Operations**: Database updates in batches
- **Job Queuing**: Prevents overlapping jobs and provides better resource management
- **Graceful Shutdown**: Proper cleanup and cancellation handling
- **Progress Tracking**: Real-time progress updates for long-running jobs
- **AI Model Optimization**: Uses Perplexity sonar model with web search for accurate founder/CEOs discovery
- **Caching**: Domain results cached for repeated lookups
- **Memory Management**: Streaming for large result sets

### Job Queuing System

The service implements a sophisticated job queuing system that provides:

- **Conflict Prevention**: Multiple jobs cannot process the same leads simultaneously
- **Resource Management**: Controlled concurrency and batching
- **Progress Tracking**: Real-time progress updates and statistics
- **Job Lifecycle**: Complete job status management (pending → running → completed/failed)
- **Cancellation Support**: Ability to cancel running jobs gracefully
- **Historical Tracking**: Job history and performance analytics

#### Job States
- **pending**: Job created but not yet started
- **running**: Job is currently executing
- **paused**: Job execution paused (future feature)
- **completed**: Job finished successfully
- **failed**: Job failed with errors
- **cancelled**: Job was cancelled by user

#### Job Locking
- Each job acquires a lock to prevent concurrent execution
- Jobs are automatically unlocked when completed or failed
- Lock conflicts are detected and prevented at job creation time


## 🎯 Usage

### Running the Service

#### Development
```bash
python -m src.main
```

#### Production (with Docker)
```bash
docker build -t email-enrichment .
docker run --env-file .env email-enrichment
```

#### As a Background Service
```bash
# Install as Windows service
python -m src.main --install-service

# Or run with PM2/process manager
pm2 start ecosystem.config.js
```

### Job-Based Processing

The service now uses a job queue system to prevent overlapping jobs and provide better batching control.

#### Process All Leads
```bash
# Create and execute a job to process all leads
python -m src.main process-all --max-concurrent 5 --batch-size 20

# Dry run to see what would be processed
python -m src.main process-all --dry-run
```

#### Process Specific Leads
```bash
# Process specific leads by ID
python -m src.main process-leads "uuid1" "uuid2" "uuid3" --max-concurrent 3

# Process leads by domain
python -m src.main process-domains "example.com" "another.com" --batch-size 10

# Process leads from CSV file
python -m src.main process-csv leads.csv --max-concurrent 5 --batch-size 20 --limit 100
```

#### CSV File Processing
```bash
# Create a sample CSV file for testing
python -m src.main create-sample-csv

# Validate CSV file without processing
python -m src.main process-csv leads.csv --dry-run

# Process CSV with limits
python -m src.main process-csv leads.csv --limit 50 --max-concurrent 3
```

#### Job Management
```bash
# View job queue status
python -m src.main job-status

# View specific job details
python -m src.main job-status "job-uuid"

# Execute an existing job
python -m src.main run-job "job-uuid"

# Cancel a running job
python -m src.main cancel-job "job-uuid"
```

#### View Statistics
```bash
# Show database statistics
python -m src.main stats

# Test API connections
python -m src.main test-connection
```

## 📊 Monitoring & Logging

### Log Levels
- **DEBUG**: Detailed API request/response logs
- **INFO**: Processing progress and results
- **WARNING**: API rate limits, temporary failures
- **ERROR**: Critical failures requiring attention

### Metrics Tracked
- Total leads processed
- Success rate per enrichment method
- API call counts and costs
- Processing time per lead
- Database operation performance

### Health Checks
- `/health` endpoint for service status
- `/metrics` endpoint for Prometheus metrics
- Database connection monitoring
- API key validity checks

## 🔒 Security Considerations

### API Key Management
- Store keys in environment variables only
- Rotate keys regularly
- Monitor API usage for anomalies

### Data Privacy
- No sensitive data logging
- GDPR compliance for email data
- Secure database connections (SSL/TLS)

### Rate Limiting
- Respect API provider limits
- Implement local rate limiting
- Exponential backoff on failures

## 🧪 Testing

### Unit Tests
```bash
pytest tests/ -v
```

### Integration Tests
```bash
pytest tests/ --integration
```

### Load Testing
```bash
# Simulate processing 1000 leads
python -m tests.load_test --leads 1000
```

## 🚨 Troubleshooting

### Common Issues

#### Hunter.io API Errors
- **Error 401**: Check API key validity
- **Error 429**: Rate limit exceeded, implement backoff
- **Error 402**: Upgrade plan or check billing

#### Perplexity API Issues
- **Timeout**: Increase request timeout in config
- **Rate Limited**: Reduce concurrent requests
- **Invalid Response**: Update parsing logic

#### Database Connection
- **Connection Timeout**: Check network and credentials
- **Permission Denied**: Verify service role key
- **Table Not Found**: Run database migrations

### Debug Mode
```bash
# Enable detailed logging
LOG_LEVEL=DEBUG python -m src.main

# Test individual components
python -c "from src.hunter_client import HunterClient; print(HunterClient().test_connection())"
```

## 📈 Scaling Considerations

### Horizontal Scaling
- Stateless service design
- Redis for distributed locking
- Message queues for job distribution

### Performance Tuning
- Adjust `MAX_WORKERS` based on API limits
- Increase `BATCH_SIZE` for better throughput
- Monitor memory usage with large datasets

### Cost Optimization
- Cache frequent domain lookups
- Prioritize high-confidence results
- Implement usage quotas per customer

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For support and questions:
- Create an issue in the repository
- Check the troubleshooting section
- Review logs for error details

## 🚀 Quick Deployment to Render

### 1. Push to GitHub
```bash
git add .
git commit -m "Initial commit"
git push origin main
```

### 2. Deploy on Render
1. Go to [render.com](https://render.com)
2. Click "New" → "Blueprint"
3. Connect your GitHub repo
4. Render will detect `render.yaml` and set up services automatically

### 3. Configure Environment Variables
In Render dashboard, set these environment variables:
- `SUPABASE_URL`
- `SUPABASE_ANON_KEY`
- `SUPABASE_SERVICE_ROLE_KEY`
- `HUNTER_IO_API_KEY`
- `PERPLEXITY_API_KEY`

### 4. Monitor & Use
- Worker starts automatically processing emails
- Health checks available at `/health`
- Statistics at `/stats`
- View logs in Render dashboard

## 🛠️ Development Scripts

### Windows (PowerShell)
```powershell
# Setup environment
.\scripts\run.ps1 setup

# Run worker
.\scripts\run.ps1 worker

# Run health check
.\scripts\run.ps1 health

# CLI commands
.\scripts\run.ps1 cli process-all
```

### Linux/macOS (Bash)
```bash
# Setup environment
chmod +x scripts/run.sh
./scripts/run.sh setup

# Run worker
./scripts/run.sh worker

# Run health check
./scripts/run.sh health

# CLI commands
./scripts/run.sh cli process-all
```

## 📊 Health Check Endpoints

When running the health check service:

- **`GET /health`** - Overall service health status
- **`GET /stats`** - Database and processing statistics

Example response:
```json
{
  "status": "healthy",
  "database": "connected",
  "jobs_queue": "operational",
  "stats": {
    "total_leads": 150,
    "total_validated_emails": 89,
    "average_confidence_score": 87.5
  }
}
```

## 🔧 Configuration Options

| Variable | Default | Description |
|----------|---------|-------------|
| `LOG_LEVEL` | INFO | Logging level (DEBUG, INFO, WARNING, ERROR) |
| `MAX_WORKERS` | 5 | Maximum concurrent API calls |
| `BATCH_SIZE` | 10 | Size of processing batches |
| `REQUEST_TIMEOUT` | 30 | HTTP request timeout in seconds |
| `HUNTER_RATE_LIMIT` | 10 | Hunter.io requests per minute |
| `PERPLEXITY_RATE_LIMIT` | 30 | Perplexity requests per minute |

## 🐳 Docker Usage

### Build
```bash
docker build -t email-enrichment .
```

### Run
```bash
docker run --env-file .env -e LOG_LEVEL=DEBUG email-enrichment
```

### Override Command
```bash
docker run --env-file .env email-enrichment python -m src.main cli process-all --max-concurrent 2
```

---

**Note**: This service processes email data. Ensure compliance with relevant data protection regulations (GDPR, CCPA, etc.) in your jurisdiction.
