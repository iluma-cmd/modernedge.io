"""
Email validation orchestrator combining Hunter.io verification with business logic
"""
import asyncio
from typing import List, Optional

from loguru import logger

from config import get_settings
from hunter_client import get_hunter_client
from models import EmailResult, GeneratedEmail, ValidatedEmail, VerificationResult


class EmailValidator:
    """Orchestrates email validation using Hunter.io and applies business logic"""

    def __init__(self):
        self.settings = get_settings()
        self.hunter_client = None

    async def _get_hunter_client(self):
        """Lazy load Hunter client"""
        if self.hunter_client is None:
            self.hunter_client = await get_hunter_client()
        return self.hunter_client

    def _determine_email_type(self, email: str, verification: VerificationResult) -> str:
        """
        Determine if email should be classified as primary or secondary

        Args:
            email: Email address
            verification: Verification result

        Returns:
            "primary" or "secondary"
        """
        # High-confidence, deliverable emails are primary
        if verification.result == "deliverable" and verification.score >= 90:
            return "primary"

        # Lower confidence but still valid emails are secondary
        return "secondary"

    def _determine_email_type_from_result(self, email_result) -> str:
        """
        Determine email type for Hunter-sourced emails (without verification)

        Args:
            email_result: EmailResult from Hunter domain search

        Returns:
            "primary" or "secondary"
        """
        # If email has personal information (first/last name), classify as primary
        if hasattr(email_result, 'first_name') and email_result.first_name:
            return "primary"

        # Otherwise, classify as secondary
        return "secondary"

    def _should_skip_email(self, email: str, existing_emails: List[str]) -> bool:
        """
        Check if email should be skipped (already exists or is generic)

        Args:
            email: Email to check
            existing_emails: List of existing emails for this lead

        Returns:
            True if email should be skipped
        """
        email_lower = email.lower()

        # Skip if already exists (filter out None values)
        existing_emails_filtered = [e for e in existing_emails if e is not None]
        if email_lower in [e.lower() for e in existing_emails_filtered]:
            return True

        # Skip generic patterns - expanded list of common generic emails
        generic_prefixes = [
            # Basic contact emails
            "info@", "support@", "contact@", "hello@", "admin@",
            "sales@", "help@", "noreply@", "no-reply@", "marketing@",
            "team@", "office@", "mail@", "email@", "news@",
            "updates@", "notifications@", "alerts@",

            # Business roles (often generic)
            "hr@", "recruiting@", "careers@", "jobs@", "employment@",
            "inquiry@", "inquiries@", "question@", "questions@",
            "feedback@", "comments@", "suggestions@",

            # Technical/support
            "tech@", "technical@", "it@", "webmaster@", "postmaster@",
            "root@", "abuse@", "security@", "privacy@", "legal@",

            # Common business emails
            "accounts@", "billing@", "finance@", "accounting@", "payroll@",
            "orders@", "shipping@", "returns@", "service@", "customer@",

            # Generic role-based
            "ceo@", "president@", "director@", "manager@", "assistant@",
            "reception@", "frontdesk@", "welcome@", "guest@", "visitor@",

            # Communication
            "press@", "media@", "pr@", "communications@", "newsletter@",
            "announcements@", "events@", "calendar@"
        ]

        for prefix in generic_prefixes:
            if email_lower.startswith(prefix):
                logger.debug(f"Skipping generic email: {email}")
                return True

        return False

    def _is_personal_email(self, email_result: EmailResult) -> bool:
        """
        Check if email has personal information (name, position)
        Hunter emails without personal details are less valuable

        Args:
            email_result: EmailResult from Hunter.io

        Returns:
            True if email has personal information
        """
        # Must have at least a first name or last name
        has_name = bool(email_result.first_name or email_result.last_name)

        # Must have some position/role information
        has_position = bool(email_result.position and email_result.position.strip())

        # Prefer emails with both name and position
        return has_name and has_position

    async def validate_hunter_emails(self, emails: List[EmailResult], existing_emails: List[str] = None) -> List[ValidatedEmail]:
        """
        Validate emails found directly from Hunter.io domain search

        Args:
            emails: EmailResult objects from Hunter domain search
            existing_emails: List of existing emails to avoid duplicates

        Returns:
            List of ValidatedEmail objects
        """
        if not emails:
            return []

        if existing_emails is None:
            existing_emails = []

        hunter_client = await self._get_hunter_client()
        validated_emails = []

        logger.debug(f"Validating {len(emails)} Hunter emails")

        # First, filter out emails we should skip and prioritize personal emails
        personal_emails = []
        other_emails = []

        for email_result in emails:
            logger.debug(f"Processing email_result: {email_result}, email attr: {getattr(email_result, 'email', 'NO EMAIL')}")
            # Skip generic emails entirely
            if self._should_skip_email(email_result.email, existing_emails):
                continue

            # Separate personal emails from others
            if self._is_personal_email(email_result):
                personal_emails.append(email_result)
            else:
                other_emails.append(email_result)

        # Prioritize personal emails, but include others if we don't have enough personal ones
        filtered_emails = personal_emails + other_emails[:max(0, 5 - len(personal_emails))]  # Max 5 emails total

        if not filtered_emails:
            logger.debug("No Hunter emails to validate after filtering")
            return []

        # For emails from Hunter domain search, skip additional verification
        # since Hunter already validated them to be on the domain
        logger.debug(f"Processing {len(filtered_emails)} Hunter-sourced emails (skipping verification)")

        for email_result in filtered_emails:
            # Create validated email directly from Hunter results
            # Assume high confidence since these came directly from Hunter domain search
            validated_email = ValidatedEmail(
                email=email_result.email,
                source="hunter_direct",
                confidence_score=85,  # High confidence for Hunter-sourced emails
                email_type=self._determine_email_type_from_result(email_result),
                hunter_status="deliverable",  # Assume deliverable since found by Hunter
                status="active",
                first_name=email_result.first_name,
                last_name=email_result.last_name,
                position=email_result.position
            )

            validated_emails.append(validated_email)
            logger.debug(f"Processed Hunter email: {email_result.email} (deliverable)")

        logger.info(f"Processed {len(validated_emails)}/{len(filtered_emails)} Hunter emails")
        return validated_emails

    async def validate_generated_emails(self, emails: List[GeneratedEmail], existing_emails: List[str] = None) -> List[ValidatedEmail]:
        """
        Validate emails generated from Perplexity name search

        Args:
            emails: GeneratedEmail objects from email generator
            existing_emails: List of existing emails to avoid duplicates

        Returns:
            List of ValidatedEmail objects
        """
        if not emails:
            return []

        if existing_emails is None:
            existing_emails = []

        hunter_client = await self._get_hunter_client()
        validated_emails = []

        logger.debug(f"Validating {len(emails)} generated emails")

        # First, filter out emails we should skip
        filtered_emails = []
        for generated_email in emails:
            if not self._should_skip_email(generated_email.email, existing_emails):
                filtered_emails.append(generated_email)

        if not filtered_emails:
            logger.debug("No generated emails to validate after filtering")
            return []

        # Verify emails in batches
        email_addresses = [e.email for e in filtered_emails]
        verification_results = await hunter_client.verify_emails_batch(email_addresses)

        # Process results
        for generated_email, verification in zip(filtered_emails, verification_results):
            # Check if verification meets criteria
            if not hunter_client.is_verification_acceptable(verification):
                logger.debug(f"Generated email {generated_email.email} failed verification criteria")
                continue

            # Create validated email
            validated_email_data = {
                "email": generated_email.email,
                "source": "perplexity_generated",
                "confidence_score": verification.score,
                "email_type": self._determine_email_type(generated_email.email, verification),
                "hunter_status": verification.result,
                "status": "active"
            }

            # Only set verified_at if it's available and not None
            if hasattr(verification, 'verified_at') and verification.verified_at is not None:
                validated_email_data["verified_at"] = verification.verified_at

            validated_email = ValidatedEmail(**validated_email_data)

            validated_emails.append(validated_email)
            logger.debug(f"Validated generated email: {generated_email.email} ({verification.result})")

        logger.info(f"Validated {len(validated_emails)}/{len(filtered_emails)} generated emails")
        return validated_emails

    async def validate_emails_combined(
        self,
        hunter_emails: List[EmailResult] = None,
        generated_emails: List[GeneratedEmail] = None,
        existing_emails: List[str] = None,
        max_emails_per_lead: int = 5
    ) -> List[ValidatedEmail]:
        """
        Validate both Hunter and generated emails, returning the best ones

        Args:
            hunter_emails: Emails from Hunter domain search
            generated_emails: Emails generated from Perplexity
            existing_emails: Existing emails to avoid
            max_emails_per_lead: Maximum emails to return

        Returns:
            List of validated emails, prioritized by quality
        """
        if existing_emails is None:
            existing_emails = []

        all_validated = []

        # Validate Hunter emails first (higher priority)
        if hunter_emails:
            hunter_validated = await self.validate_hunter_emails(hunter_emails, existing_emails)
            all_validated.extend(hunter_validated)

        # Then validate generated emails
        if generated_emails:
            generated_validated = await self.validate_generated_emails(generated_emails, existing_emails)
            all_validated.extend(generated_validated)

        # Sort by confidence score (highest first)
        all_validated.sort(key=lambda x: x.confidence_score, reverse=True)

        # Limit results
        result = all_validated[:max_emails_per_lead]

        # Update existing_emails to include newly validated ones
        new_emails = [v.email for v in result]
        existing_emails.extend(new_emails)

        logger.info(f"Combined validation: {len(result)} emails validated from {len(hunter_emails or []) + len(generated_emails or [])} candidates")
        return result

    async def validate_single_email(self, email: str) -> Optional[ValidatedEmail]:
        """
        Validate a single email address

        Args:
            email: Email address to validate

        Returns:
            ValidatedEmail object or None if invalid
        """
        hunter_client = await self._get_hunter_client()

        try:
            verification = await hunter_client.verify_email(email)

            if not hunter_client.is_verification_acceptable(verification):
                logger.debug(f"Email {email} failed validation criteria")
                return None

            validated_email = ValidatedEmail(
                email=email,
                source="manual_validation",  # Special source for manual validation
                confidence_score=verification.score,
                email_type=self._determine_email_type(email, verification),
                hunter_status=verification.result,
                status="active"
            )

            logger.debug(f"Validated single email: {email} ({verification.result})")
            return validated_email

        except Exception as e:
            logger.error(f"Failed to validate single email {email}: {e}")
            return None


# Global validator instance
email_validator = EmailValidator()


async def get_email_validator() -> EmailValidator:
    """Get the global email validator instance"""
    return email_validator
