"""
Email permutation generator for creating possible email addresses
"""
import re
from typing import List

from loguru import logger

from models import GeneratedEmail, PersonInfo


class EmailGenerator:
    """Generate email address permutations from names and domains"""

    def __init__(self):
        # Comprehensive email patterns prioritized by likelihood
        self.patterns = [
            # Most common patterns (prioritized first)
            "firstname.lastname",      # john.doe@company.com
            "firstname_lastname",      # john_doe@company.com
            "f.lastname",              # j.doe@company.com
            "firstname.l",             # john.d@company.com
            "firstname",               # john@company.com
            "lastname.firstname",      # doe.john@company.com
            "lastname_firstname",      # doe_john@company.com
            "l.firstname",             # d.john@company.com
            "lastname.f",              # doe.j@company.com
            "lastname",                # doe@company.com

            # Common variations
            "firstnamelastname",       # johndoe@company.com
            "lastnamefirstname",       # doejohn@company.com
            "f_lastname",              # j_doe@company.com
            "firstname.l",             # john.d@company.com (duplicate but different)
            "firstnamel",              # johnd@company.com
            "lfirstname",              # djohn@company.com

            # Initial-based patterns
            "ff.lastname",             # jj.doe@company.com (for John James Doe)
            "firstname.ff",            # john.jd@company.com
            "f.f.lastname",            # j.j.doe@company.com
            "ff.lastname",             # jd.doe@company.com

            # Number variations (common in some companies)
            "firstname.lastname1",     # john.doe1@company.com
            "firstname_lastname1",     # john_doe1@company.com
            "f.lastname1",             # j.doe1@company.com

            # Department/role indicators (less common but sometimes used)
            "firstname.lastname.admin",  # john.doe.admin@company.com
            "firstname.lastname.info",   # john.doe.info@company.com

            # Alternative separators
            "firstname-lastname",      # john-doe@company.com
            "f-lastname",              # j-doe@company.com
            "lastname-firstname",      # doe-john@company.com
        ]

        # Pattern priorities for scoring (higher = more likely)
        self.pattern_priorities = {
            "firstname.lastname": 100,
            "firstname_lastname": 95,
            "f.lastname": 90,
            "firstname.l": 85,
            "firstname": 80,
            "lastname.firstname": 75,
            "lastname_firstname": 70,
            "l.firstname": 65,
            "lastname.f": 60,
            "lastname": 55,
            # Lower priorities for other patterns
            "firstnamelastname": 40,
            "lastnamefirstname": 35,
            "f_lastname": 50,
            "firstnamel": 45,
            "lfirstname": 30,
            "ff.lastname": 25,
            "firstname.ff": 20,
            "f.f.lastname": 15,
            "ff.lastname": 10,
            "firstname.lastname1": 5,
            "firstname_lastname1": 5,
            "f.lastname1": 5,
            "firstname-lastname": 45,
            "f-lastname": 40,
            "lastname-firstname": 35,
        }

    def _clean_name(self, name: str) -> str:
        """
        Clean and normalize a name for email generation

        Args:
            name: The name to clean

        Returns:
            Cleaned name string
        """
        if not name:
            return ""

        # Remove special characters and extra spaces
        cleaned = re.sub(r'[^\w\s-]', '', name)
        cleaned = re.sub(r'\s+', '', cleaned)

        # Convert to lowercase
        return cleaned.lower()

    def _generate_pattern_emails(self, first_name: str, last_name: str, domain: str) -> List[GeneratedEmail]:
        """
        Generate email addresses using predefined patterns

        Args:
            first_name: Cleaned first name
            last_name: Cleaned last name
            domain: Domain name

        Returns:
            List of GeneratedEmail objects
        """
        emails = []
        domain = domain.lower().strip()

        for pattern in self.patterns:
            try:
                if pattern == "firstname.lastname":
                    local_part = f"{first_name}.{last_name}"
                elif pattern == "firstname.l":
                    local_part = f"{first_name}.{last_name[0] if last_name else ''}"
                elif pattern == "f.lastname":
                    local_part = f"{first_name[0] if first_name else ''}.{last_name}"
                elif pattern == "firstname":
                    local_part = first_name
                elif pattern == "lastname":
                    local_part = last_name
                elif pattern == "firstname_lastname":
                    local_part = f"{first_name}_{last_name}"
                elif pattern == "f_lastname":
                    local_part = f"{first_name[0] if first_name else ''}_{last_name}"
                elif pattern == "firstnamel":
                    local_part = f"{first_name}{last_name[0] if last_name else ''}"
                elif pattern == "lastname.firstname":
                    local_part = f"{last_name}.{first_name}"
                elif pattern == "l.firstname":
                    local_part = f"{last_name[0] if last_name else ''}.{first_name}"
                elif pattern == "lastname.f":
                    local_part = f"{last_name}.{first_name[0] if first_name else ''}"
                elif pattern == "lastfirstname":
                    local_part = f"{last_name}{first_name}"
                else:
                    continue  # Skip unknown patterns

                # Skip empty local parts
                if not local_part or local_part == "." or local_part == "_":
                    continue

                # Create full email
                email_address = f"{local_part}@{domain}"

                # Validate email format (basic check)
                if self._is_valid_email_format(email_address):
                    emails.append(GeneratedEmail(
                        email=email_address,
                        pattern=pattern,
                        source="perplexity_generated"
                    ))

            except (IndexError, AttributeError) as e:
                logger.debug(f"Failed to generate email for pattern {pattern}: {e}")
                continue

        return emails

    def _is_valid_email_format(self, email: str) -> bool:
        """
        Basic email format validation

        Args:
            email: Email address to validate

        Returns:
            True if format is valid
        """
        # Simple regex for basic email validation
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'

        if not re.match(pattern, email):
            return False

        # Additional checks
        local_part, domain_part = email.split('@', 1)

        # Local part should not be empty and not start/end with dot
        if not local_part or local_part.startswith('.') or local_part.endswith('.'):
            return False

        # Domain should not start/end with dot or hyphen
        if domain_part.startswith('.') or domain_part.endswith('.') or domain_part.startswith('-') or domain_part.endswith('-'):
            return False

        # No consecutive dots
        if '..' in email:
            return False

        return True

    def _remove_duplicates(self, emails: List[GeneratedEmail]) -> List[GeneratedEmail]:
        """
        Remove duplicate email addresses, keeping the first occurrence

        Args:
            emails: List of generated emails

        Returns:
            List with duplicates removed
        """
        seen = set()
        unique_emails = []

        for email in emails:
            if email.email not in seen:
                seen.add(email.email)
                unique_emails.append(email)

        return unique_emails

    def _prioritize_emails(self, emails: List[GeneratedEmail]) -> List[GeneratedEmail]:
        """
        Prioritize email patterns by likelihood

        Args:
            emails: List of generated emails

        Returns:
            Sorted list with most likely patterns first
        """
        # Priority order (most common to least common)
        priority_patterns = [
            "firstname.lastname",     # Most common
            "f.lastname",            # Common abbreviation
            "firstname",             # firstname@domain
            "firstname_lastname",    # Underscore variant
            "lastname.firstname",    # Less common but still used
            "firstname.l",           # firstname with last initial
            "lastfirstname",         # Concatenated
            "lastname",              # lastname@domain
            "f_lastname",            # Underscore with abbreviation
            "firstnamel",            # firstname with last initial no dot
            "l.firstname",           # last initial dot firstname
            "lastname.f",            # lastname dot first initial
        ]

        def get_priority(email: GeneratedEmail) -> int:
            try:
                return priority_patterns.index(email.pattern)
            except ValueError:
                return len(priority_patterns)  # Lowest priority for unknown patterns

        # Sort by priority (lower number = higher priority)
        return sorted(emails, key=get_priority)

    def generate_email_permutations(self, person_info: PersonInfo, domain: str, max_emails: int = 10) -> List[GeneratedEmail]:
        """
        Generate email address permutations for a person

        Args:
            person_info: PersonInfo object with name details
            domain: Company domain
            max_emails: Maximum number of emails to generate

        Returns:
            List of GeneratedEmail objects
        """
        if not person_info or not domain:
            logger.warning("Missing person info or domain for email generation")
            return []

        # Clean the names (but not the domain - keep dots)
        first_name = self._clean_name(person_info.first_name)
        last_name = self._clean_name(person_info.last_name)
        domain = domain.lower().strip()  # Just lowercase and strip domain

        if not first_name and not last_name:
            logger.warning("No valid names found for email generation")
            return []

        logger.debug(f"Generating emails for {first_name} {last_name}@{domain}")

        # Generate emails using patterns
        emails = self._generate_pattern_emails(first_name, last_name, domain)

        # Remove duplicates
        emails = self._remove_duplicates(emails)

        # Prioritize by likelihood
        emails = self._prioritize_emails(emails)

        # Limit the number of results
        result = emails[:max_emails]

        logger.info(f"Generated {len(result)} email permutations for {first_name} {last_name}@{domain}")
        return result

    def generate_emails_batch(self, person_infos: List[PersonInfo], domains: List[str], max_emails_per_person: int = 10) -> List[List[GeneratedEmail]]:
        """
        Generate email permutations for multiple people

        Args:
            person_infos: List of PersonInfo objects
            domains: List of corresponding domains
            max_emails_per_person: Maximum emails per person

        Returns:
            List of lists containing GeneratedEmail objects for each person
        """
        if len(person_infos) != len(domains):
            raise ValueError("person_infos and domains lists must have the same length")

        results = []
        for person_info, domain in zip(person_infos, domains):
            if person_info:
                emails = self.generate_email_permutations(person_info, domain, max_emails_per_person)
                results.append(emails)
            else:
                results.append([])

        total_emails = sum(len(emails) for emails in results)
        logger.info(f"Generated {total_emails} emails for {len(person_infos)} people")

        return results


# Global email generator instance
email_generator = EmailGenerator()


def get_email_generator() -> EmailGenerator:
    """Get the global email generator instance"""
    return email_generator
