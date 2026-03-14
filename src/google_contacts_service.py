"""Google Contacts service implementation for MCP server."""

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from config import config
from credential_manager import CredentialManager


class GoogleContactsError(Exception):
    """Exception raised for errors in the Google Contacts service."""

    pass


class GoogleContactsService:
    """Service to interact with Google Contacts API."""

    # Extended person fields for comprehensive contact information
    PERSON_FIELDS = [
        "names",
        "emailAddresses",
        "phoneNumbers",
        "addresses",
        "birthdays",
        "organizations",
        "occupations",
        "urls",
        "biographies",
        "relations",
        "nicknames",
        "events",
        "userDefined",
        "sipAddresses",
        "imClients",
        "photos",
        "memberships",
        "miscKeywords",
        "interests",
        "skills",
        "braggingRights",
        "taglines",
        "coverPhotos",
        "locales",
        "externalIds",
    ]

    def __init__(
        self,
        credentials_info: Optional[Dict[str, Any]] = None,
        token_path: Optional[Path] = None,
        cred_manager: Optional[CredentialManager] = None,
    ):
        """Initialize the Google Contacts service with credentials info.

        Args:
            credentials_info: OAuth client credentials information
            token_path: Path to store the token file (unused when cred_manager is set)
            cred_manager: Optional credential manager for reading/writing the refresh token
        """
        self.credentials_info = credentials_info
        self.token_path = token_path or config.token_path
        self.cred_manager = cred_manager
        self.service = self._authenticate()

    @classmethod
    def from_file(
        cls, credentials_path: Union[str, Path], token_path: Optional[Path] = None
    ) -> "GoogleContactsService":
        """Create service instance from a credentials file.

        Args:
            credentials_path: Path to the credentials.json file
            token_path: Optional custom path to store the token

        Returns:
            Configured GoogleContactsService instance

        Raises:
            GoogleContactsError: If credentials file cannot be read
        """
        try:
            # Load the credentials from the provided file
            with open(credentials_path, "r") as file:
                credentials_info = json.load(file)

            return cls(credentials_info, token_path)
        except (json.JSONDecodeError, IOError) as e:
            raise GoogleContactsError(
                f"Failed to load credentials from {credentials_path}: {str(e)}"
            )

    @classmethod
    def from_env(cls, token_path: Optional[Path] = None) -> "GoogleContactsService":
        """Create service instance from environment variables.

        Args:
            token_path: Optional custom path to store the token

        Returns:
            Configured GoogleContactsService instance

        Raises:
            GoogleContactsError: If required environment variables are missing
        """
        client_id = os.environ.get("GOOGLE_CLIENT_ID") or config.google_client_id
        client_secret = os.environ.get("GOOGLE_CLIENT_SECRET") or config.google_client_secret

        if not client_id or not client_secret:
            raise GoogleContactsError(
                "Missing Google API credentials. Set GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET "
                "environment variables or provide a credentials file."
            )

        # Build credentials info from environment variables
        credentials_info = {
            "installed": {
                "client_id": client_id,
                "client_secret": client_secret,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                "redirect_uris": ["urn:ietf:wg:oauth:2.0:oob", "http://localhost"],
            }
        }

        return cls(credentials_info, token_path)

    @classmethod
    def from_cred_manager(
        cls,
        cred_manager: CredentialManager,
        token_path: Optional[Path] = None,
    ) -> "GoogleContactsService":
        """Create service instance using a pluggable credential manager.

        Reads client_id and client_secret via the credential manager.
        The refresh token is also read (and written after re-auth) via the
        credential manager instead of token.json.

        Args:
            cred_manager: A CredentialManager implementation (e.g. OnePasswordCredentialManager)
            token_path:   Ignored — token is stored in the credential manager, not on disk.
                          Accepted for API consistency only.

        Returns:
            Configured GoogleContactsService instance

        Raises:
            GoogleContactsError: If client_id or client_secret cannot be retrieved
        """
        client_id = cred_manager.get("client_id")
        client_secret = cred_manager.get("client_secret")

        if not client_id or not client_secret:
            raise GoogleContactsError(
                "Credential manager could not supply client_id and/or client_secret."
            )

        credentials_info = {
            "installed": {
                "client_id": client_id,
                "client_secret": client_secret,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                "redirect_uris": ["urn:ietf:wg:oauth:2.0:oob", "http://localhost"],
            }
        }

        return cls(credentials_info, token_path, cred_manager=cred_manager)

    def _authenticate(self):
        """Authenticate with Google using credentials info.

        Returns:
            Authenticated Google service client

        Raises:
            GoogleContactsError: If authentication fails
        """
        try:
            creds = None
            token_path = self.token_path

            if self.cred_manager:
                # Load refresh token from credential manager (no file I/O)
                refresh_token = self.cred_manager.get("refresh_token")
                if refresh_token and self.credentials_info:
                    client_id = self.credentials_info["installed"]["client_id"]
                    client_secret = self.credentials_info["installed"]["client_secret"]
                    creds = Credentials(
                        None,
                        refresh_token=refresh_token,
                        token_uri="https://oauth2.googleapis.com/token",
                        client_id=client_id,
                        client_secret=client_secret,
                        scopes=config.scopes,
                    )
            else:
                # Ensure token directory exists
                token_path.parent.mkdir(parents=True, exist_ok=True)

                # Check if we have existing token file
                if token_path.exists():
                    with open(token_path, "r") as token_file:
                        creds = Credentials.from_authorized_user_info(
                            json.load(token_file), config.scopes
                        )

                # Check for refresh token in environment
                refresh_token = (
                    os.environ.get("GOOGLE_REFRESH_TOKEN") or config.google_refresh_token
                )
                if not creds and refresh_token and self.credentials_info:
                    client_id = self.credentials_info["installed"]["client_id"]
                    client_secret = self.credentials_info["installed"]["client_secret"]
                    creds = Credentials(
                        None,
                        refresh_token=refresh_token,
                        token_uri="https://oauth2.googleapis.com/token",
                        client_id=client_id,
                        client_secret=client_secret,
                        scopes=config.scopes,
                    )

            # If credentials don't exist or are invalid, refresh when possible before
            # falling back to the interactive browser flow.
            if not creds or not creds.valid:
                if creds and creds.refresh_token:
                    creds.refresh(Request())
                else:
                    if not self.credentials_info:
                        raise GoogleContactsError(
                            "No valid credentials found and no credentials info provided for authentication."
                        )

                    flow = InstalledAppFlow.from_client_config(self.credentials_info, config.scopes)
                    creds = flow.run_local_server(port=0)

                # Persist the new/refreshed token
                if self.cred_manager:
                    if creds.refresh_token:
                        self.cred_manager.set("refresh_token", creds.refresh_token)
                        print("Refresh token saved to credential manager.")
                else:
                    with open(token_path, "w") as token:
                        token.write(creds.to_json())

                    if creds.refresh_token:
                        print(
                            "\nNew refresh token obtained. Consider setting this in your environment:"
                        )
                        print(f"GOOGLE_REFRESH_TOKEN={creds.refresh_token}\n")

            # Build and return the Google Contacts service
            return build("people", "v1", credentials=creds)

        except Exception as e:
            raise GoogleContactsError(f"Authentication failed: {str(e)}")

    def list_contacts(
        self,
        name_filter: Optional[str] = None,
        max_results: int = None,
        include_all_fields: bool = False,
    ) -> List[Dict[str, Any]]:
        """List contacts, optionally filtering by name with pagination support.

        Args:
            name_filter: Optional filter to find contacts by name
            max_results: Maximum number of results to return
            include_all_fields: Whether to include all contact fields

        Returns:
            List of contact dictionaries

        Raises:
            GoogleContactsError: If API request fails
        """
        max_results = max_results or config.default_max_results

        try:
            contacts = []
            next_page_token = None

            # Use extended fields if requested
            person_fields = (
                ",".join(self.PERSON_FIELDS)
                if include_all_fields
                else "names,emailAddresses,phoneNumbers,addresses,organizations,birthdays,urls,biographies,relations,nicknames"
            )

            while len(contacts) < max_results:
                page_size = min(1000, max_results - len(contacts))  # Google API limit is 1000

                request_params = {
                    "resourceName": "people/me",
                    "pageSize": page_size,
                    "personFields": person_fields,
                    "sortOrder": "DISPLAY_NAME_ASCENDING",
                }

                if next_page_token:
                    request_params["pageToken"] = next_page_token

                results = self.service.people().connections().list(**request_params).execute()

                connections = results.get("connections", [])
                if not connections:
                    break

                for person in connections:
                    contact = self._format_contact_enhanced(person)

                    # Apply name filter if provided
                    if name_filter:
                        filter_lower = name_filter.lower()
                        if not any(
                            filter_lower in str(contact.get(field, "")).lower()
                            for field in ["displayName", "givenName", "familyName", "nickname"]
                        ):
                            continue

                    contacts.append(contact)

                    if len(contacts) >= max_results:
                        break

                next_page_token = results.get("nextPageToken")
                if not next_page_token:
                    break

            return contacts

        except HttpError as error:
            raise GoogleContactsError(f"Error listing contacts: {error}")

    def search_contacts(
        self, query: str, max_results: int = 50, search_fields: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Enhanced search functionality with server-side filtering and multiple field support.

        Args:
            query: Search term
            max_results: Maximum number of results
            search_fields: Specific fields to search in

        Returns:
            List of matching contact dictionaries
        """
        try:
            # Use the searchContacts API endpoint for better search
            # Note: This is a newer API that might not be available in all regions
            search_request = {
                "query": query,
                "readMask": ",".join(self.PERSON_FIELDS),
                "pageSize": min(max_results, 50),  # API limit for search
            }

            try:
                # Try the new search API first
                response = self.service.people().searchContacts(**search_request).execute()
                results = response.get("results", [])

                contacts = []
                for result in results:
                    person = result.get("person", {})
                    if person:
                        contact = self._format_contact_enhanced(person)
                        contacts.append(contact)

                return contacts[:max_results]

            except HttpError as search_error:
                # Fallback to manual search if the search API isn't available
                print(f"Search API not available, falling back to manual search: {search_error}")
                return self._manual_search_contacts(query, max_results, search_fields)

        except Exception as error:
            raise GoogleContactsError(f"Error searching contacts: {error}")

    def _manual_search_contacts(
        self, query: str, max_results: int = 50, search_fields: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Fallback manual search with enhanced field matching."""
        # Get a larger set of contacts to search through
        all_contacts = self.list_contacts(max_results=max_results * 3, include_all_fields=True)

        query_lower = query.lower()
        matches = []

        # Default search fields
        if not search_fields:
            search_fields = [
                "displayName",
                "givenName",
                "familyName",
                "nickname",
                "emails",
                "phones",
                "organization",
                "jobTitle",
            ]

        for contact in all_contacts:
            match_found = False

            for field in search_fields:
                field_value = contact.get(field, "")

                # Handle list fields (emails, phones, etc.)
                if isinstance(field_value, list):
                    for item in field_value:
                        if query_lower in str(item).lower():
                            match_found = True
                            break
                # Handle string fields
                elif field_value and query_lower in str(field_value).lower():
                    match_found = True
                    break

            if match_found:
                matches.append(contact)
                if len(matches) >= max_results:
                    break

        return matches

    def get_contact(self, identifier: str, include_all_fields: bool = True) -> Dict[str, Any]:
        """Get a contact by resource name or email with comprehensive field support.

        Args:
            identifier: Resource name (people/*) or email address
            include_all_fields: Whether to include all available fields

        Returns:
            Contact dictionary with comprehensive information
        """
        try:
            person_fields = (
                ",".join(self.PERSON_FIELDS)
                if include_all_fields
                else "names,emailAddresses,phoneNumbers,addresses,organizations"
            )

            if identifier.startswith("people/"):
                # Get by resource name
                person = (
                    self.service.people()
                    .get(resourceName=identifier, personFields=person_fields)
                    .execute()
                )

                return self._format_contact_enhanced(person)
            else:
                # Search by email
                contacts = self.search_contacts(identifier, max_results=1)
                if contacts:
                    return contacts[0]

                raise GoogleContactsError(f"Contact with identifier {identifier} not found")

        except HttpError as error:
            raise GoogleContactsError(f"Error getting contact: {error}")

    def create_contact(self, contact_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new contact with comprehensive field support.

        Args:
            contact_data: Dictionary containing contact information

        Returns:
            Created contact dictionary
        """
        try:
            contact_body = self._build_contact_body(contact_data)

            person = self.service.people().createContact(body=contact_body).execute()

            return self._format_contact_enhanced(person)

        except HttpError as error:
            raise GoogleContactsError(f"Error creating contact: {error}")

    def update_contact(self, resource_name: str, contact_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing contact with comprehensive field support.

        Args:
            resource_name: Contact resource name
            contact_data: Dictionary containing updated contact information

        Returns:
            Updated contact dictionary
        """
        try:
            # Get current contact for etag
            current_person = (
                self.service.people()
                .get(resourceName=resource_name, personFields=",".join(self.PERSON_FIELDS))
                .execute()
            )

            etag = current_person.get("etag")

            # Build update body using the same logic as create
            update_body = self._build_contact_body(contact_data, current_person)
            update_body["etag"] = etag
            update_body["resourceName"] = resource_name

            # Map input fields to API fields for updatePersonFields
            field_mapping = {
                "given_name": "names",
                "family_name": "names",
                "nickname": "nicknames",
                "email": "emailAddresses",
                "emails": "emailAddresses",
                "phone": "phoneNumbers",
                "phones": "phoneNumbers",
                "address": "addresses",
                "addresses": "addresses",
                "organization": "organizations",
                "job_title": "organizations",
                "birthday": "birthdays",
                "website": "urls",
                "urls": "urls",
                "notes": "biographies",
                "relations": "relations",
                "events": "events",
                "custom_fields": "userDefined",
            }

            # Determine which API fields to update based on input
            update_fields = set()
            for input_field in contact_data.keys():
                if input_field in field_mapping:
                    update_fields.add(field_mapping[input_field])

            if not update_fields:
                return self._format_contact_enhanced(current_person)

            # Execute update
            updated_person = (
                self.service.people()
                .updateContact(
                    resourceName=resource_name,
                    updatePersonFields=",".join(update_fields),
                    body=update_body,
                )
                .execute()
            )

            return self._format_contact_enhanced(updated_person)

        except HttpError as error:
            raise GoogleContactsError(f"Error updating contact: {error}")

    def delete_contact(self, resource_name: str) -> Dict:
        """Delete a contact by resource name."""
        try:
            self.service.people().deleteContact(resourceName=resource_name).execute()

            return {"success": True, "resourceName": resource_name}

        except HttpError as error:
            raise GoogleContactsError(f"Error deleting contact: {error}")

    def list_directory_people(
        self, query: Optional[str] = None, max_results: int = 50
    ) -> List[Dict]:
        """List people from the Google Workspace directory.

        Args:
            query: Optional search query to filter directory results
            max_results: Maximum number of results to return

        Returns:
            List of formatted directory contact dictionaries
        """
        try:
            # Check if directory API access is available
            directory_fields = "names,emailAddresses,organizations,phoneNumbers"

            # Build the request, with or without a query
            if query:
                request = self.service.people().searchDirectoryPeople(
                    query=query,
                    readMask=directory_fields,
                    sources=[
                        "DIRECTORY_SOURCE_TYPE_DOMAIN_CONTACT",
                        "DIRECTORY_SOURCE_TYPE_DOMAIN_PROFILE",
                    ],
                    pageSize=max_results,
                )
            else:
                request = self.service.people().listDirectoryPeople(
                    readMask=directory_fields,
                    sources=[
                        "DIRECTORY_SOURCE_TYPE_DOMAIN_CONTACT",
                        "DIRECTORY_SOURCE_TYPE_DOMAIN_PROFILE",
                    ],
                    pageSize=max_results,
                )

            # Execute the request
            response = request.execute()
            print("response; ", response)

            # Process the results
            people = response.get("people", [])
            if not people:
                return []

            # Format each person entry
            directory_contacts = []
            for person in people:
                contact = self._format_directory_person(person)
                directory_contacts.append(contact)

            return directory_contacts

        except HttpError as error:
            # Handle gracefully if not a Google Workspace account
            if error.resp.status == 403:
                print("Directory API access forbidden. This may not be a Google Workspace account.")
                return []
            raise Exception(f"Error listing directory people: {error}")

    def search_directory(self, query: str, max_results: int = 20) -> List[Dict]:
        """Search for people in the Google Workspace directory.

        This is a more focused search function that uses the searchDirectoryPeople endpoint.

        Args:
            query: Search query to find specific users
            max_results: Maximum number of results to return

        Returns:
            List of matching directory contact dictionaries
        """
        try:
            response = (
                self.service.people()
                .searchDirectoryPeople(
                    query=query,
                    readMask="names,emailAddresses,organizations,phoneNumbers",
                    sources=[
                        "DIRECTORY_SOURCE_TYPE_DOMAIN_CONTACT",
                        "DIRECTORY_SOURCE_TYPE_DOMAIN_PROFILE",
                    ],
                    pageSize=max_results,
                )
                .execute()
            )

            people = response.get("people", [])

            if not people:
                return []

            # Format the results
            directory_results = []
            for person in people:
                contact = self._format_directory_person(person)
                directory_results.append(contact)

            return directory_results

        except HttpError as error:
            if error.resp.status == 403:
                print(
                    "Directory search access forbidden. This may not be a Google Workspace account."
                )
                return []
            raise Exception(f"Error searching directory: {error}")

    def get_other_contacts(self, max_results: int = 100) -> List[Dict]:
        """Get contacts from the 'Other contacts' section of Google Contacts.

        These are contacts that the user has interacted with but has not added to their contacts.

        Args:
            max_results: Maximum number of results to return

        Returns:
            List of other contact dictionaries
        """
        try:
            response = (
                self.service.otherContacts()
                .list(readMask="names,emailAddresses,phoneNumbers", pageSize=max_results)
                .execute()
            )

            other_contacts = response.get("otherContacts", [])

            if not other_contacts:
                return []

            # Format the results
            contacts = []
            for person in other_contacts:
                contact = self._format_contact(person)
                contacts.append(contact)

            return contacts

        except HttpError as error:
            raise Exception(f"Error getting other contacts: {error}")

    def _format_contact(self, person: Dict) -> Dict:
        """Format a Google People API person object into a simplified contact."""
        names = person.get("names", [])
        emails = person.get("emailAddresses", [])
        phones = person.get("phoneNumbers", [])

        given_name = names[0].get("givenName", "") if names else ""
        family_name = names[0].get("familyName", "") if names else ""
        display_name = (
            names[0].get("displayName", "") if names else f"{given_name} {family_name}".strip()
        )

        return {
            "resourceName": person.get("resourceName"),
            "givenName": given_name,
            "familyName": family_name,
            "displayName": display_name,
            "email": emails[0].get("value") if emails else None,
            "phone": phones[0].get("value") if phones else None,
        }

    def _format_directory_person(self, person: Dict) -> Dict:
        """Format a Google Directory API person object into a simplified contact.

        This handles the specific format of directory contacts which may have different
        organization and other fields compared to regular contacts.
        """
        names = person.get("names", [])
        emails = person.get("emailAddresses", [])
        phones = person.get("phoneNumbers", [])
        orgs = person.get("organizations", [])

        given_name = names[0].get("givenName", "") if names else ""
        family_name = names[0].get("familyName", "") if names else ""
        display_name = (
            names[0].get("displayName", "") if names else f"{given_name} {family_name}".strip()
        )

        # Get organization details - these are often present in directory entries
        department = ""
        job_title = ""
        if orgs:
            department = orgs[0].get("department", "")
            job_title = orgs[0].get("title", "")

        return {
            "resourceName": person.get("resourceName"),
            "givenName": given_name,
            "familyName": family_name,
            "displayName": display_name,
            "email": emails[0].get("value") if emails else None,
            "phone": phones[0].get("value") if phones else None,
            "department": department,
            "jobTitle": job_title,
        }

    def _build_contact_body(
        self, contact_data: Dict[str, Any], current_person: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Build contact body for create/update operations with comprehensive field support."""
        body = {}

        # Build different sections of the contact body
        self._build_names_section(body, contact_data, current_person)
        self._build_contact_info_section(body, contact_data, current_person)
        self._build_addresses_section(body, contact_data)
        self._build_organization_section(body, contact_data, current_person)
        self._build_personal_info_section(body, contact_data)
        self._build_additional_fields_section(body, contact_data)

        return body

    def _build_names_section(
        self,
        body: Dict[str, Any],
        contact_data: Dict[str, Any],
        current_person: Optional[Dict[str, Any]],
    ) -> None:
        """Build names and nicknames section of contact body."""
        # Names
        if "given_name" in contact_data or "family_name" in contact_data:
            names = []
            if current_person and current_person.get("names"):
                names = current_person["names"].copy()

            if not names:
                names = [{}]

            if "given_name" in contact_data:
                names[0]["givenName"] = contact_data["given_name"]
            if "family_name" in contact_data:
                names[0]["familyName"] = contact_data["family_name"]

            body["names"] = names

        # Nicknames
        if "nickname" in contact_data:
            body["nicknames"] = [{"value": contact_data["nickname"]}]

    def _build_contact_info_section(
        self,
        body: Dict[str, Any],
        contact_data: Dict[str, Any],
        current_person: Optional[Dict[str, Any]],
    ) -> None:
        """Build email and phone sections of contact body."""
        self._build_email_addresses(body, contact_data, current_person)
        self._build_phone_numbers(body, contact_data, current_person)

    def _build_email_addresses(
        self,
        body: Dict[str, Any],
        contact_data: Dict[str, Any],
        current_person: Optional[Dict[str, Any]],
    ) -> None:
        """Build email addresses section of contact body."""
        if "emails" in contact_data:
            emails = []
            for email_data in contact_data["emails"]:
                if isinstance(email_data, str):
                    emails.append({"value": email_data})
                else:
                    emails.append(email_data)
            body["emailAddresses"] = emails
        elif "email" in contact_data:
            # For single email updates, preserve existing emails or create new
            if current_person and current_person.get("emailAddresses"):
                emails = current_person["emailAddresses"].copy()
                # Update the first email or add if none exist
                if emails:
                    emails[0]["value"] = contact_data["email"]
                else:
                    emails = [{"value": contact_data["email"]}]
            else:
                emails = [{"value": contact_data["email"]}]
            body["emailAddresses"] = emails

    def _build_phone_numbers(
        self,
        body: Dict[str, Any],
        contact_data: Dict[str, Any],
        current_person: Optional[Dict[str, Any]],
    ) -> None:
        """Build phone numbers section of contact body."""
        if "phones" in contact_data:
            phones = []
            for phone_data in contact_data["phones"]:
                if isinstance(phone_data, str):
                    phones.append({"value": phone_data})
                else:
                    phones.append(phone_data)
            body["phoneNumbers"] = phones
        elif "phone" in contact_data:
            # For single phone updates, preserve existing phones or create new
            if current_person and current_person.get("phoneNumbers"):
                phones = current_person["phoneNumbers"].copy()
                # Update the first phone or add if none exist
                if phones:
                    phones[0]["value"] = contact_data["phone"]
                else:
                    phones = [{"value": contact_data["phone"]}]
            else:
                phones = [{"value": contact_data["phone"]}]
            body["phoneNumbers"] = phones

    def _build_addresses_section(self, body: Dict[str, Any], contact_data: Dict[str, Any]) -> None:
        """Build addresses section of contact body."""
        if "addresses" in contact_data:
            addresses = []
            for addr_data in contact_data["addresses"]:
                if isinstance(addr_data, str):
                    addresses.append({"formattedValue": addr_data})
                else:
                    addresses.append(addr_data)
            body["addresses"] = addresses
        elif "address" in contact_data:
            body["addresses"] = [{"formattedValue": contact_data["address"]}]

    def _build_organization_section(
        self,
        body: Dict[str, Any],
        contact_data: Dict[str, Any],
        current_person: Optional[Dict[str, Any]],
    ) -> None:
        """Build organization section of contact body."""
        if "organization" in contact_data or "job_title" in contact_data:
            # Preserve existing organization data when updating
            if current_person and current_person.get("organizations"):
                org = current_person["organizations"][0].copy()
            else:
                org = {}

            if "organization" in contact_data:
                org["name"] = contact_data["organization"]
            if "job_title" in contact_data:
                org["title"] = contact_data["job_title"]
            body["organizations"] = [org]

    def _build_personal_info_section(
        self, body: Dict[str, Any], contact_data: Dict[str, Any]
    ) -> None:
        """Build personal info section (birthday, URLs, notes) of contact body."""
        # Birthday
        if "birthday" in contact_data:
            birthday_data = contact_data["birthday"]
            if isinstance(birthday_data, str):
                # Parse string format like "1990-01-15"
                parts = birthday_data.split("-")
                if len(parts) == 3:
                    body["birthdays"] = [
                        {
                            "date": {
                                "year": int(parts[0]),
                                "month": int(parts[1]),
                                "day": int(parts[2]),
                            }
                        }
                    ]
            else:
                body["birthdays"] = [birthday_data]

        # URLs
        if "urls" in contact_data:
            urls = []
            for url_data in contact_data["urls"]:
                if isinstance(url_data, str):
                    urls.append({"value": url_data})
                else:
                    urls.append(url_data)
            body["urls"] = urls
        elif "website" in contact_data:
            body["urls"] = [{"value": contact_data["website"]}]

        # Biography/Notes
        if "notes" in contact_data:
            body["biographies"] = [{"value": contact_data["notes"]}]

    def _build_additional_fields_section(
        self, body: Dict[str, Any], contact_data: Dict[str, Any]
    ) -> None:
        """Build additional fields section (relations, events, custom fields) of contact body."""
        # Relations
        if "relations" in contact_data:
            relations = []
            for rel_data in contact_data["relations"]:
                if isinstance(rel_data, str):
                    relations.append({"person": rel_data})
                else:
                    relations.append(rel_data)
            body["relations"] = relations

        # Events (like anniversaries)
        if "events" in contact_data:
            body["events"] = contact_data["events"]

        # Custom fields
        if "custom_fields" in contact_data:
            body["userDefined"] = contact_data["custom_fields"]

    def _format_contact_enhanced(self, person: Dict[str, Any]) -> Dict[str, Any]:
        """Format a Google People API person object into a comprehensive contact dictionary."""
        contact = {"resourceName": person.get("resourceName"), "etag": person.get("etag")}

        # Format different sections of the contact
        self._format_names_data(contact, person)
        self._format_contact_data(contact, person)
        self._format_organization_data(contact, person)
        self._format_personal_data(contact, person)
        self._format_additional_data(contact, person)

        return contact

    def _format_names_data(self, contact: Dict[str, Any], person: Dict[str, Any]) -> None:
        """Format names and nicknames data from person object."""
        # Names
        names = person.get("names", [])
        if names:
            name = names[0]
            contact.update(
                {
                    "givenName": name.get("givenName", ""),
                    "familyName": name.get("familyName", ""),
                    "displayName": name.get("displayName", ""),
                    "middleName": name.get("middleName", ""),
                    "honorificPrefix": name.get("honorificPrefix", ""),
                    "honorificSuffix": name.get("honorificSuffix", ""),
                }
            )

        # Nicknames
        nicknames = person.get("nicknames", [])
        if nicknames:
            contact["nickname"] = nicknames[0].get("value", "")

    def _format_contact_data(self, contact: Dict[str, Any], person: Dict[str, Any]) -> None:
        """Format contact information (emails, phones, addresses) from person object."""
        # Email addresses
        emails = person.get("emailAddresses", [])
        contact["emails"] = []
        for email in emails:
            contact["emails"].append(
                {
                    "value": email.get("value", ""),
                    "type": email.get("type", ""),
                    "label": email.get("formattedType", ""),
                }
            )
        # Keep backward compatibility
        if emails:
            contact["email"] = emails[0].get("value", "")

        # Phone numbers
        phones = person.get("phoneNumbers", [])
        contact["phones"] = []
        for phone in phones:
            contact["phones"].append(
                {
                    "value": phone.get("value", ""),
                    "type": phone.get("type", ""),
                    "label": phone.get("formattedType", ""),
                }
            )
        # Keep backward compatibility
        if phones:
            contact["phone"] = phones[0].get("value", "")

        # Addresses
        addresses = person.get("addresses", [])
        contact["addresses"] = []
        for addr in addresses:
            contact["addresses"].append(
                {
                    "formatted": addr.get("formattedValue", ""),
                    "type": addr.get("type", ""),
                    "street": addr.get("streetAddress", ""),
                    "city": addr.get("city", ""),
                    "region": addr.get("region", ""),
                    "postal_code": addr.get("postalCode", ""),
                    "country": addr.get("country", ""),
                }
            )

    def _format_organization_data(self, contact: Dict[str, Any], person: Dict[str, Any]) -> None:
        """Format organization data from person object."""
        organizations = person.get("organizations", [])
        if organizations:
            org = organizations[0]
            contact.update(
                {
                    "organization": org.get("name", ""),
                    "jobTitle": org.get("title", ""),
                    "department": org.get("department", ""),
                }
            )

    def _format_personal_data(self, contact: Dict[str, Any], person: Dict[str, Any]) -> None:
        """Format personal data (birthday, URLs, notes) from person object."""
        # Birthday
        birthdays = person.get("birthdays", [])
        if birthdays:
            birthday = birthdays[0].get("date", {})
            if birthday:
                contact["birthday"] = {
                    "year": birthday.get("year"),
                    "month": birthday.get("month"),
                    "day": birthday.get("day"),
                }

        # URLs
        urls = person.get("urls", [])
        contact["urls"] = []
        for url in urls:
            contact["urls"].append(
                {
                    "value": url.get("value", ""),
                    "type": url.get("type", ""),
                    "label": url.get("formattedType", ""),
                }
            )

        # Biography/Notes
        biographies = person.get("biographies", [])
        if biographies:
            contact["notes"] = biographies[0].get("value", "")

    def _format_additional_data(self, contact: Dict[str, Any], person: Dict[str, Any]) -> None:
        """Format additional data (relations, events, custom fields, etc.) from person object."""
        # Relations
        relations = person.get("relations", [])
        contact["relations"] = []
        for relation in relations:
            contact["relations"].append(
                {
                    "person": relation.get("person", ""),
                    "type": relation.get("type", ""),
                    "label": relation.get("formattedType", ""),
                }
            )

        # Events
        events = person.get("events", [])
        contact["events"] = []
        for event in events:
            event_data = {"type": event.get("type", ""), "label": event.get("formattedType", "")}
            if event.get("date"):
                event_data["date"] = event["date"]
            contact["events"].append(event_data)

        # Custom fields
        custom_fields = person.get("userDefined", [])
        contact["customFields"] = []
        for field in custom_fields:
            contact["customFields"].append(
                {"key": field.get("key", ""), "value": field.get("value", "")}
            )

        # Photos
        photos = person.get("photos", [])
        if photos:
            contact["photoUrl"] = photos[0].get("url", "")

        # Memberships (contact groups)
        memberships = person.get("memberships", [])
        contact["groups"] = []
        for membership in memberships:
            contact["groups"].append(
                {
                    "resourceName": membership.get("contactGroupMembership", {}).get(
                        "contactGroupResourceName", ""
                    )
                }
            )

    def list_contact_groups(self, include_system_groups: bool = True) -> List[Dict[str, Any]]:
        """List all contact groups owned by the authenticated user.

        Args:
            include_system_groups: Whether to include system contact groups

        Returns:
            List of contact group dictionaries
        """
        try:
            response = self.service.contactGroups().list().execute()
            contact_groups = response.get("contactGroups", [])

            if not include_system_groups:
                contact_groups = [
                    group
                    for group in contact_groups
                    if group.get("groupType") == "USER_CONTACT_GROUP"
                ]

            formatted_groups = []
            for group in contact_groups:
                formatted_group = self._format_contact_group(group)
                formatted_groups.append(formatted_group)

            return formatted_groups

        except HttpError as error:
            raise GoogleContactsError(f"Error listing contact groups: {error}")

    def create_contact_group(
        self, name: str, client_data: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        """Create a new contact group.

        Args:
            name: Name for the new contact group
            client_data: Optional client-specific data

        Returns:
            Created contact group dictionary
        """
        try:
            contact_group_body = {"contactGroup": {"name": name}}

            if client_data:
                contact_group_body["contactGroup"]["clientData"] = client_data

            response = self.service.contactGroups().create(body=contact_group_body).execute()
            return self._format_contact_group(response)

        except HttpError as error:
            raise GoogleContactsError(f"Error creating contact group: {error}")

    def get_contact_group(self, resource_name: str, max_members: int = 0) -> Dict[str, Any]:
        """Get a specific contact group by resource name.

        Args:
            resource_name: Contact group resource name (contactGroups/*)
            max_members: Maximum number of members to return (0 for metadata only)

        Returns:
            Contact group dictionary with member details
        """
        try:
            params = {}
            if max_members > 0:
                params["maxMembers"] = max_members

            response = (
                self.service.contactGroups().get(resourceName=resource_name, **params).execute()
            )

            return self._format_contact_group(response, include_members=max_members > 0)

        except HttpError as error:
            raise GoogleContactsError(f"Error getting contact group: {error}")

    def update_contact_group(
        self, resource_name: str, name: str, client_data: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        """Update a contact group's name and client data.

        Args:
            resource_name: Contact group resource name
            name: New name for the contact group
            client_data: Optional updated client data

        Returns:
            Updated contact group dictionary
        """
        try:
            # Get current group for etag
            current_group = self.service.contactGroups().get(resourceName=resource_name).execute()

            contact_group_body = {
                "contactGroup": {
                    "resourceName": resource_name,
                    "etag": current_group.get("etag"),
                    "name": name,
                },
                "updateGroupFields": "name",
            }

            if client_data:
                contact_group_body["contactGroup"]["clientData"] = client_data
                contact_group_body["updateGroupFields"] = "name,clientData"

            response = (
                self.service.contactGroups()
                .update(contactGroup_resourceName=resource_name, body=contact_group_body)
                .execute()
            )

            return self._format_contact_group(response)

        except HttpError as error:
            raise GoogleContactsError(f"Error updating contact group: {error}")

    def delete_contact_group(self, resource_name: str) -> Dict[str, Any]:
        """Delete a contact group.

        Args:
            resource_name: Contact group resource name

        Returns:
            Success status dictionary
        """
        try:
            self.service.contactGroups().delete(resourceName=resource_name).execute()
            return {"success": True, "resourceName": resource_name}

        except HttpError as error:
            raise GoogleContactsError(f"Error deleting contact group: {error}")

    def add_contacts_to_group(
        self, group_resource_name: str, contact_resource_names: List[str]
    ) -> Dict[str, Any]:
        """Add contacts to a contact group.

        Args:
            group_resource_name: Contact group resource name
            contact_resource_names: List of contact resource names to add

        Returns:
            Result dictionary with any errors
        """
        try:
            modify_body = {"resourceNamesToAdd": contact_resource_names}

            response = (
                self.service.contactGroups()
                .members()
                .modify(resourceName=group_resource_name, body=modify_body)
                .execute()
            )

            return {
                "success": True,
                "added_count": len(contact_resource_names),
                "not_found": response.get("notFoundResourceNames", []),
                "could_not_add": response.get("canNotRemoveLastContactGroupResourceNames", []),
            }

        except HttpError as error:
            raise GoogleContactsError(f"Error adding contacts to group: {error}")

    def remove_contacts_from_group(
        self, group_resource_name: str, contact_resource_names: List[str]
    ) -> Dict[str, Any]:
        """Remove contacts from a contact group.

        Args:
            group_resource_name: Contact group resource name
            contact_resource_names: List of contact resource names to remove

        Returns:
            Result dictionary with any errors
        """
        try:
            modify_body = {"resourceNamesToRemove": contact_resource_names}

            response = (
                self.service.contactGroups()
                .members()
                .modify(resourceName=group_resource_name, body=modify_body)
                .execute()
            )

            return {
                "success": True,
                "removed_count": len(contact_resource_names),
                "not_found": response.get("notFoundResourceNames", []),
                "could_not_remove": response.get("canNotRemoveLastContactGroupResourceNames", []),
            }

        except HttpError as error:
            raise GoogleContactsError(f"Error removing contacts from group: {error}")

    def _format_contact_group(
        self, group: Dict[str, Any], include_members: bool = False
    ) -> Dict[str, Any]:
        """Format a contact group dictionary for display.

        Args:
            group: Raw contact group data from API
            include_members: Whether to include member details

        Returns:
            Formatted contact group dictionary
        """
        formatted_group = {
            "resourceName": group.get("resourceName", ""),
            "name": group.get("name", ""),
            "formattedName": group.get("formattedName", ""),
            "groupType": group.get("groupType", ""),
            "memberCount": group.get("memberCount", 0),
        }

        # Add metadata if available
        if group.get("metadata"):
            metadata = group["metadata"]
            formatted_group.update(
                {
                    "updateTime": metadata.get("updateTime", ""),
                    "deleted": metadata.get("deleted", False),
                }
            )

        # Add client data if available
        if group.get("clientData"):
            formatted_group["clientData"] = group["clientData"]

        # Add member resource names if requested and available
        if include_members and group.get("memberResourceNames"):
            formatted_group["memberResourceNames"] = group["memberResourceNames"]

        return formatted_group
