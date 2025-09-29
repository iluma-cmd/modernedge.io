"""
Hunter.io API client for domain search and email verification
"""
import asyncio
from typing import List, Optional

import httpx
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from config import get_settings
from models import EmailResult, HunterDomainResponse, VerificationResult


class HunterAPIError(Exception):
    """Custom exception for Hunter.io API errors"""
    pass


class HunterRateLimitError(HunterAPIError):
    """Exception for rate limit errors"""
    pass


class HunterClient:
    """Hunter.io API client with rate limiting and error handling"""

    BASE_URL = "https://api.hunter.io/v2"

    def __init__(self):
        self.settings = get_settings()
        self.api_key = self.settings.hunter_api_key
        self.rate_limit = self.settings.hunter_rate_limit
        self.confidence_threshold = self.settings.hunter_confidence_threshold
        self.verification_threshold = self.settings.verification_confidence_threshold

        # Rate limiting
        self._request_times: List[float] = []
        self._rate_limit_lock = asyncio.Lock()

        # HTTP client
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client"""
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=self.settings.request_timeout,
                headers={
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
                    logger.warning(f"Rate limit reached, waiting {wait_time:.2f} seconds")
                    await asyncio.sleep(wait_time)

            # Record this request
            self._request_times.append(current_time)

    def _handle_api_error(self, response: httpx.Response) -> None:
        """Handle API errors and raise appropriate exceptions"""
        if response.status_code == 401:
            raise HunterAPIError("Invalid API key")
        elif response.status_code == 403:
            raise HunterAPIError("API access forbidden - check your plan")
        elif response.status_code == 404:
            raise HunterAPIError("Resource not found")
        elif response.status_code == 429:
            raise HunterRateLimitError("Rate limit exceeded")
        elif response.status_code >= 500:
            raise HunterAPIError(f"Server error: {response.status_code}")
        elif not response.is_success:
            raise HunterAPIError(f"API error: {response.status_code} - {response.text}")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.ConnectError, HunterRateLimitError))
    )
    async def domain_search(self, domain: str) -> Optional[HunterDomainResponse]:
        """
        Search for emails associated with a domain using Hunter.io Domain Search API

        Args:
            domain: The domain to search for (e.g., "example.com")

        Returns:
            HunterDomainResponse object or None if no results
        """
        await self._enforce_rate_limit()

        try:
            client = await self._get_client()
            url = f"{self.BASE_URL}/domain-search"
            params = {
                "domain": domain,
                "api_key": self.api_key,
                "limit": min(10, self.settings.max_emails_per_domain * 2)  # Don't exceed plan limit, get 2x for filtering
            }

            logger.debug(f"Making domain search request for {domain}")
            response = await client.get(url, params=params)
            self._handle_api_error(response)

            data = response.json()
            logger.debug(f"Domain search response for {domain}: {len(data.get('data', {}).get('emails', []))} emails found")

            # Parse response
            domain_data = data.get("data", {})
            if not domain_data:
                logger.warning(f"No domain data found for {domain}")
                return None

            # Filter emails by confidence threshold
            emails = []
            for email_data in domain_data.get("emails", []):
                confidence = email_data.get("confidence", 0)
                if confidence >= self.confidence_threshold:
                    # Extract source domains from source objects
                    sources = email_data.get("sources", [])
                    if sources and isinstance(sources[0], dict):
                        # Sources are objects with 'domain' field
                        source_domains = [source.get("domain", "") for source in sources if source.get("domain")]
                    else:
                        # Sources are already strings
                        source_domains = [str(source) for source in sources]

                    email_result = EmailResult(
                        email=email_data["value"],
                        first_name=email_data.get("first_name"),
                        last_name=email_data.get("last_name"),
                        position=email_data.get("position"),
                        seniority=email_data.get("seniority"),
                        department=email_data.get("department"),
                        linkedin_url=email_data.get("linkedin"),
                        twitter_username=email_data.get("twitter"),
                        confidence_score=confidence,
                        sources=source_domains
                    )
                    emails.append(email_result)

            # Sort emails by confidence (highest first)
            emails.sort(key=lambda x: x.confidence_score, reverse=True)

            domain_response = HunterDomainResponse(
                domain=domain_data.get("domain", domain),
                disposable=domain_data.get("disposable", False),
                webmail=domain_data.get("webmail", False),
                accept_all=domain_data.get("accept_all", False),
                pattern=domain_data.get("pattern"),
                organization=domain_data.get("organization"),
                description=domain_data.get("description"),
                industry=domain_data.get("industry"),
                twitter=domain_data.get("twitter"),
                facebook=domain_data.get("facebook"),
                linkedin=domain_data.get("linkedin"),
                instagram=domain_data.get("instagram"),
                youtube=domain_data.get("youtube"),
                emails=emails[:self.settings.max_emails_per_domain],  # Limit results
                country=domain_data.get("country")
            )

            logger.info(f"Domain search successful for {domain}: {len(emails)} emails found above threshold")
            return domain_response

        except HunterRateLimitError:
            logger.warning(f"Rate limit hit for domain search on {domain}, will retry")
            raise
        except Exception as e:
            logger.error(f"Domain search failed for {domain}: {e}")
            raise HunterAPIError(f"Domain search failed: {str(e)}")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.ConnectError, HunterRateLimitError))
    )
    async def verify_email(self, email: str) -> VerificationResult:
        """
        Verify an email address using Hunter.io Email Verifier API

        Args:
            email: The email address to verify

        Returns:
            VerificationResult object
        """
        await self._enforce_rate_limit()

        try:
            client = await self._get_client()
            url = f"{self.BASE_URL}/email-verifier"
            params = {
                "email": email,
                "api_key": self.api_key
            }

            logger.debug(f"Verifying email: {email}")
            response = await client.get(url, params=params)
            self._handle_api_error(response)

            data = response.json()
            verification_data = data.get("data", {})

            # Extract source domains from source objects
            sources = verification_data.get("sources", [])
            if sources and isinstance(sources[0], dict):
                # Sources are objects with 'domain' field
                source_domains = [source.get("domain", "") for source in sources if source.get("domain")]
            else:
                # Sources are already strings
                source_domains = [str(source) for source in sources]

            verification_result = VerificationResult(
                email=email,
                result=verification_data.get("result", "unknown"),
                score=verification_data.get("score", 0),
                disposable=verification_data.get("disposable", False),
                webmail=verification_data.get("webmail", False),
                mx_records=verification_data.get("mx_records", False),
                smtp_server=verification_data.get("smtp_server", False),
                smtp_check=verification_data.get("smtp_check", False),
                accept_all=verification_data.get("accept_all", False),
                block=verification_data.get("block", False),
                gibberish=verification_data.get("gibberish", False),
                role=verification_data.get("role", False),
                sources=source_domains
            )

            logger.debug(f"Email verification result for {email}: {verification_result.result} (score: {verification_result.score})")
            return verification_result

        except HunterRateLimitError:
            logger.warning(f"Rate limit hit for email verification on {email}, will retry")
            raise
        except Exception as e:
            logger.error(f"Email verification failed for {email}: {e}")
            raise HunterAPIError(f"Email verification failed: {str(e)}")

    async def verify_emails_batch(self, emails: List[str]) -> List[VerificationResult]:
        """
        Verify multiple emails sequentially to respect rate limits

        Args:
            emails: List of email addresses to verify

        Returns:
            List of VerificationResult objects
        """
        if not emails:
            return []

        logger.info(f"Verifying batch of {len(emails)} emails sequentially")

        verified_emails = []
        successful_verifications = 0

        for email in emails:
            try:
                result = await self.verify_email(email)
                verified_emails.append(result)
                if result.result == "deliverable":
                    successful_verifications += 1
                logger.debug(f"Verified {email}: {result.result}")

            except Exception as e:
                logger.error(f"Failed to verify email {email}: {e}")
                # Return a failed verification result
                verified_emails.append(VerificationResult(
                    email=email,
                    result="unknown",
                    score=0,
                    gibberish=False,
                    role=False
                ))

        logger.info(f"Batch verification complete: {successful_verifications}/{len(emails)} emails deliverable")

        return verified_emails

    def is_email_quality_acceptable(self, email_result: EmailResult) -> bool:
        """
        Check if an email result meets quality criteria

        Args:
            email_result: EmailResult from domain search

        Returns:
            True if email meets quality criteria
        """
        # Check confidence threshold
        if email_result.confidence_score < self.confidence_threshold:
            return False

        # Prefer role-based emails over generic ones
        email_lower = email_result.email.lower()

        # Generic patterns to avoid
        generic_patterns = [
            "info@", "support@", "contact@", "hello@", "admin@",
            "sales@", "help@", "noreply@", "no-reply@"
        ]

        for pattern in generic_patterns:
            if email_lower.startswith(pattern):
                return False

        # Prefer emails with position information
        if email_result.position:
            # Prioritize executive roles
            executive_roles = ["ceo", "founder", "owner", "president", "director", "vp", "head"]
            position_lower = email_result.position.lower()
            if any(role in position_lower for role in executive_roles):
                return True

        return True

    def is_verification_acceptable(self, verification: VerificationResult) -> bool:
        """
        Check if email verification meets acceptance criteria

        Args:
            verification: VerificationResult from email verifier

        Returns:
            True if verification meets criteria
        """
        # Must be deliverable or risky (but not undeliverable)
        if verification.result not in ["deliverable", "risky"]:
            return False

        # Must meet confidence threshold
        if verification.score < self.verification_threshold:
            return False

        # Avoid disposable and blocked emails
        if verification.disposable or verification.block:
            return False

        # For lenient mode (Hunter-sourced emails), allow accept-all domains if score is high enough
        if verification.accept_all and verification.score < 75:
            return False

        # Avoid gibberish emails (random character emails)
        if verification.gibberish:
            return False

        # Note: We don't automatically reject role-based emails here
        # because some role-based emails (like ceo@, founder@) might be valuable
        # The filtering happens in the email quality check instead

        return True

    async def close(self):
        """Close HTTP client connections"""
        if self._client:
            await self._client.aclose()
            self._client = None
            logger.info("Hunter.io client closed")


# Global Hunter client instance
hunter_client = HunterClient()


async def get_hunter_client() -> HunterClient:
    """Get the global Hunter.io client instance"""
    return hunter_client
