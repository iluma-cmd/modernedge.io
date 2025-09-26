"""
Perplexity AI API client for finding owner/founder names
"""
import asyncio
import json
import re
from typing import Optional

import httpx
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from config import get_settings
from models import PerplexityResponse, PersonInfo


class PerplexityAPIError(Exception):
    """Custom exception for Perplexity API errors"""
    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.status_code = status_code


class PerplexityRateLimitError(PerplexityAPIError):
    """Exception for rate limit errors"""
    pass


class PerplexityBadRequestError(PerplexityAPIError):
    """Exception for bad request errors (invalid parameters)"""
    pass


class PerplexityAPIStatusError(PerplexityAPIError):
    """Exception for API status errors"""
    pass


class PerplexityClient:
    """Perplexity AI API client with rate limiting and error handling"""

    BASE_URL = "https://api.perplexity.ai"

    def __init__(self):
        self.settings = get_settings()
        self.api_key = self.settings.perplexity_api_key
        self.rate_limit = self.settings.perplexity_rate_limit

        # Rate limiting
        self._request_times: list[float] = []
        self._rate_limit_lock = asyncio.Lock()

        # HTTP client
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client"""
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=self.settings.request_timeout,
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "Email-Enrichment-Service/1.0"
                }
            )
        return self._client

    async def _enforce_rate_limit(self):
        """Enforce rate limiting to stay within API limits"""
        async with self._rate_limit_lock:
            current_time = asyncio.get_event_loop().time()

            # Remove requests older than 1 minute
            self._request_times = [
                t for t in self._request_times
                if current_time - t < 60
            ]

            # Check if we're at the limit
            if len(self._request_times) >= self.rate_limit:
                # Wait until we can make another request
                oldest_request = min(self._request_times)
                wait_time = 60 - (current_time - oldest_request)
                if wait_time > 0:
                    logger.warning(f"Perplexity rate limit reached, waiting {wait_time:.2f} seconds")
                    await asyncio.sleep(wait_time)

            # Record this request
            self._request_times.append(current_time)

    def _handle_api_error(self, response: httpx.Response) -> None:
        """Handle API errors and raise appropriate exceptions according to documentation"""
        if response.status_code == 400:
            raise PerplexityBadRequestError(f"Bad request - invalid parameters: {response.text}", 400)
        elif response.status_code == 401:
            raise PerplexityAPIError("Invalid API key", 401)
        elif response.status_code == 403:
            raise PerplexityAPIError("API access forbidden", 403)
        elif response.status_code == 429:
            raise PerplexityRateLimitError("Rate limit exceeded", 429)
        elif response.status_code >= 500:
            raise PerplexityAPIStatusError(f"Server error: {response.status_code}", response.status_code)
        elif not response.is_success:
            raise PerplexityAPIStatusError(f"API error: {response.status_code} - {response.text}", response.status_code)

    def _build_prompt(self, company_domain: str) -> str:
        """
        Build the prompt for finding owner/founder information

        Args:
            company_domain: The company domain to research

        Returns:
            Formatted prompt string
        """
        return f"""Find the owner, founder, or CEO of the company with domain {company_domain}.

Please provide:
1. Full name of the primary decision-maker
2. Their title/position
3. The company name they are associated with

Focus on finding the most senior executive or founder. If multiple people are found, prioritize the CEO, founder, or president.

Format your response as:
Name: [Full Name]
Title: [Position]
Company: [Company Name]

Be precise and only include information you can verify."""

    def _parse_person_info(self, response_text: str) -> Optional[PersonInfo]:
        """
        Parse person information from Perplexity response

        Args:
            response_text: Raw response from Perplexity

        Returns:
            PersonInfo object or None if parsing fails
        """
        try:
            # Look for structured format in response
            name_match = re.search(r'Name:\s*([^\n]+)', response_text, re.IGNORECASE)
            title_match = re.search(r'Title:\s*([^\n]+)', response_text, re.IGNORECASE)
            company_match = re.search(r'Company:\s*([^\n]+)', response_text, re.IGNORECASE)

            if not name_match:
                logger.warning(f"Could not extract name from response: {response_text[:200]}...")
                return None

            full_name = name_match.group(1).strip()

            # Split full name into first and last
            name_parts = full_name.split()
            if len(name_parts) < 2:
                logger.warning(f"Full name doesn't have enough parts: {full_name}")
                return None

            first_name = name_parts[0]
            last_name = " ".join(name_parts[1:])  # Handle multi-part last names

            # Clean up the names
            first_name = re.sub(r'[^\w\s-]', '', first_name).strip()
            last_name = re.sub(r'[^\w\s-]', '', last_name).strip()

            if not first_name or not last_name:
                logger.warning(f"Invalid name parts: first='{first_name}', last='{last_name}'")
                return None

            person_info = PersonInfo(
                first_name=first_name,
                last_name=last_name,
                title=title_match.group(1).strip() if title_match else None,
                company=company_match.group(1).strip() if company_match else None,
                confidence=0.8  # Default confidence for Perplexity responses
            )

            logger.debug(f"Parsed person info: {person_info.first_name} {person_info.last_name}")
            return person_info

        except Exception as e:
            logger.error(f"Failed to parse person info from response: {e}")
            return None

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.ConnectError, PerplexityRateLimitError, PerplexityAPIStatusError))
    )
    async def find_person_name(self, company_domain: str) -> Optional[PersonInfo]:
        """
        Find the owner/founder name for a company domain using Perplexity AI

        Args:
            company_domain: The company domain to research

        Returns:
            PersonInfo object with name details or None if not found
        """
        await self._enforce_rate_limit()

        try:
            client = await self._get_client()
            url = f"{self.BASE_URL}/chat/completions"

            prompt = self._build_prompt(company_domain)

            # Use Bearer token authentication
            headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}

            payload = {
                "model": "sonar",  # Using the standard model for general queries
                "messages": [
                    {
                        "role": "system",
                        "content": "You are an expert research assistant specializing in business information. Always provide accurate, well-sourced information about company leadership and founders."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                "max_tokens": 500,
                "temperature": 0.2,  # Slightly higher temperature for balanced creativity and accuracy
                "top_p": 0.9,
                "presence_penalty": 0.0,  # No penalty for factual business info
                "frequency_penalty": 0.0,
                "web_search_options": {
                    "search_recency_filter": "year",  # Focus on recent information
                    "max_search_results": 5
                }
            }

            logger.debug(f"Making Perplexity request for domain: {company_domain}")
            response = await client.post(url, headers=headers, json=payload)
            self._handle_api_error(response)

            data = response.json()

            # Extract the response content according to documented structure
            if "choices" not in data or not data["choices"]:
                logger.warning(f"No choices in Perplexity response for {company_domain}")
                return None

            choice = data["choices"][0]
            if "message" not in choice or "content" not in choice["message"]:
                logger.warning(f"Invalid response structure for {company_domain}")
                return None

            content = choice["message"]["content"]
            usage = data.get("usage")
            request_id = data.get("id")  # Request ID from response

            perplexity_response = PerplexityResponse(
                content=content,
                usage=usage,
                request_id=request_id
            )

            logger.debug(f"Perplexity response for {company_domain} (ID: {request_id}): {content[:200]}...")

            # Parse the person information
            person_info = self._parse_person_info(content)

            if person_info:
                logger.info(f"Successfully found person info for {company_domain}: {person_info.first_name} {person_info.last_name}")
            else:
                logger.warning(f"Could not parse person info from Perplexity response for {company_domain}")

            return person_info

        except PerplexityRateLimitError:
            logger.warning(f"Rate limit hit for Perplexity request on {company_domain}, will retry")
            raise
        except Exception as e:
            logger.error(f"Perplexity request failed for {company_domain}: {e}")
            raise PerplexityAPIError(f"Perplexity request failed: {str(e)}")

    async def find_linkedin_profile(self, person_name: str, company_domain: str) -> Optional[str]:
        """
        Find LinkedIn profile URL for a person

        Args:
            person_name: Full name of the person
            company_domain: Company domain for context

        Returns:
            LinkedIn profile URL if found, None otherwise
        """
        if not person_name or not person_name.strip():
            return None

        await self._enforce_rate_limit()

        try:
            client = await self._get_client()
            url = f"{self.BASE_URL}/chat/completions"

            prompt = f"""Find the LinkedIn profile URL for {person_name.strip()} who works at or is associated with {company_domain}.

Please provide only the LinkedIn profile URL in the format: https://www.linkedin.com/in/username
If you cannot find a definitive LinkedIn profile, respond with "NOT_FOUND".
Do not include any other text or explanation."""

            # Use Bearer token authentication
            headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}

            payload = {
                "model": "sonar",
                "messages": [
                    {
                        "role": "system",
                        "content": "You are an expert research assistant specializing in finding professional LinkedIn profiles. Always provide accurate LinkedIn URLs when available."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                "max_tokens": 200,
                "temperature": 0.1,  # Low temperature for factual URL responses
                "top_p": 0.9,
                "web_search_options": {
                    "search_recency_filter": "year",
                    "max_search_results": 3
                }
            }

            logger.debug(f"Making LinkedIn profile search for {person_name}")
            response = await client.post(url, headers=headers, json=payload)
            self._handle_api_error(response)

            data = response.json()
            content = data.get('choices', [{}])[0].get('message', {}).get('content', '').strip()

            # Check if it's a valid LinkedIn URL
            if content.startswith('https://www.linkedin.com/in/') and 'NOT_FOUND' not in content:
                return content
            else:
                logger.debug(f"No LinkedIn profile found for {person_name}")
                return None

        except Exception as e:
            logger.warning(f"Failed to find LinkedIn profile for {person_name}: {e}")
            return None

    async def find_company_linkedin(self, company_domain: str) -> Optional[str]:
        """
        Find LinkedIn company page URL

        Args:
            company_domain: Company domain

        Returns:
            LinkedIn company page URL if found, None otherwise
        """
        if not company_domain or not company_domain.strip():
            return None

        # Extract company name from domain (remove .com, .org, etc.)
        company_name = company_domain.split('.')[0].replace('-', ' ').title()

        await self._enforce_rate_limit()

        try:
            client = await self._get_client()
            url = f"{self.BASE_URL}/chat/completions"

            prompt = f"""Find the official LinkedIn company page URL for {company_name} (domain: {company_domain}).

Please provide only the LinkedIn company page URL in the format: https://www.linkedin.com/company/companyname
If you cannot find the official company page, respond with "NOT_FOUND".
Do not include any other text or explanation."""

            # Use Bearer token authentication
            headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}

            payload = {
                "model": "sonar",
                "messages": [
                    {
                        "role": "system",
                        "content": "You are an expert research assistant specializing in finding official company LinkedIn pages. Always provide accurate LinkedIn company URLs when available."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                "max_tokens": 200,
                "temperature": 0.1,  # Low temperature for factual URL responses
                "top_p": 0.9,
                "web_search_options": {
                    "search_recency_filter": "year",
                    "max_search_results": 3
                }
            }

            logger.debug(f"Making LinkedIn company search for {company_domain}")
            response = await client.post(url, headers=headers, json=payload)
            self._handle_api_error(response)

            data = response.json()
            content = data.get('choices', [{}])[0].get('message', {}).get('content', '').strip()

            # Check if it's a valid LinkedIn company URL
            if content.startswith('https://www.linkedin.com/company/') and 'NOT_FOUND' not in content:
                return content
            else:
                logger.debug(f"No LinkedIn company page found for {company_domain}")
                return None

        except Exception as e:
            logger.warning(f"Failed to find LinkedIn company page for {company_domain}: {e}")
            return None

    async def find_person_names_batch(self, company_domains: list[str]) -> list[Optional[PersonInfo]]:
        """
        Find person names for multiple company domains concurrently

        Args:
            company_domains: List of company domains to research

        Returns:
            List of PersonInfo objects (or None for failures)
        """
        if not company_domains:
            return []

        logger.info(f"Finding person names for batch of {len(company_domains)} domains")

        # Create semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(self.settings.max_workers)

        async def find_with_semaphore(domain: str) -> Optional[PersonInfo]:
            async with semaphore:
                return await self.find_person_name(domain)

        # Process domains concurrently
        tasks = [find_with_semaphore(domain) for domain in company_domains]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Handle exceptions and collect results
        person_infos = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Failed to find person for domain {company_domains[i]}: {result}")
                person_infos.append(None)
            else:
                person_infos.append(result)

        successful_finds = len([p for p in person_infos if p is not None])
        logger.info(f"Batch person finding complete: {successful_finds}/{len(company_domains)} successful")

        return person_infos

    async def close(self):
        """Close HTTP client connections"""
        if self._client:
            await self._client.aclose()
            self._client = None
            logger.info("Perplexity client closed")


# Global Perplexity client instance
perplexity_client = PerplexityClient()


async def get_perplexity_client() -> PerplexityClient:
    """Get the global Perplexity client instance"""
    return perplexity_client
