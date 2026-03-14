"""MCP tools implementation for Google Contacts."""

import traceback
from typing import Any, Dict, List, Optional

from mcp.server.fastmcp import FastMCP

from config import config
from formatters import (
    format_contact,
    format_contact_group,
    format_contact_groups_list,
    format_contacts_list,
    format_directory_people,
    format_group_membership_result,
)
from google_contacts_service import GoogleContactsError, GoogleContactsService
from credential_manager import CredentialManager

# Global service instance
contacts_service = None
# Optional credential manager (set before init_service() is called)
_cred_manager: "CredentialManager | None" = None


def init_service() -> Optional[GoogleContactsService]:
    """Initialize and return a Google Contacts service instance.

    Returns:
        GoogleContactsService instance or None if initialization fails
    """
    global contacts_service

    if contacts_service:
        return contacts_service

    try:
        # Try credential manager first (e.g. 1Password)
        if _cred_manager is not None:
            try:
                contacts_service = GoogleContactsService.from_cred_manager(_cred_manager)
                print("Successfully loaded credentials from credential manager.")
                return contacts_service
            except GoogleContactsError as e:
                print(f"Credential manager failed: {e}")

        # Then try environment variables
        try:
            contacts_service = GoogleContactsService.from_env()
            print("Successfully loaded credentials from environment variables.")
            return contacts_service
        except GoogleContactsError:
            pass

        # Then try default file locations
        for path in config.credentials_paths:
            if path.exists():
                try:
                    print(f"Found credentials file at {path}")
                    contacts_service = GoogleContactsService.from_file(path)
                    print("Successfully loaded credentials from file.")
                    return contacts_service
                except GoogleContactsError as e:
                    print(f"Error with credentials at {path}: {e}")
                    continue

        print("No valid credentials found. Please provide credentials to use Google Contacts.")
        return None

    except Exception as e:
        print(f"Error initializing Google Contacts service: {str(e)}")
        traceback.print_exc()
        return None


def register_tools(mcp: FastMCP) -> None:
    """Register all Google Contacts tools with the MCP server.

    Args:
        mcp: FastMCP server instance
    """
    register_contact_tools(mcp)
    register_directory_tools(mcp)
    register_contact_group_tools(mcp)


def register_contact_tools(mcp: FastMCP) -> None:
    """Register contact management tools with the MCP server."""

    @mcp.tool()
    async def list_contacts(
        name_filter: Optional[str] = None, max_results: int = 100, include_all_fields: bool = False
    ) -> str:
        """List all contacts or filter by name with comprehensive field support.

        Args:
            name_filter: Optional filter to find contacts by name
            max_results: Maximum number of results to return (default: 100)
            include_all_fields: Whether to include all contact fields like addresses, birthdays, etc.
        """
        service = init_service()
        if not service:
            return "Error: Google Contacts service is not available. Please check your credentials."

        try:
            contacts = service.list_contacts(name_filter, max_results, include_all_fields)
            return format_contacts_list(contacts)
        except Exception as e:
            return f"Error: Failed to list contacts - {str(e)}"

    @mcp.tool()
    async def search_contacts(
        query: str, max_results: int = 50, search_fields: Optional[List[str]] = None
    ) -> str:
        """Enhanced search contacts by name, email, phone, organization, or other fields.

        This uses server-side search when available and falls back to comprehensive client-side search.

        Args:
            query: Search term to find in contacts
            max_results: Maximum number of results to return (default: 50)
            search_fields: Specific fields to search in (e.g., ['emails', 'phones', 'organization'])
        """
        service = init_service()
        if not service:
            return "Error: Google Contacts service is not available. Please check your credentials."

        try:
            contacts = service.search_contacts(query, max_results, search_fields)

            if not contacts:
                return f"No contacts found matching '{query}'."

            return f"Search results for '{query}':\n\n{format_contacts_list(contacts)}"
        except Exception as e:
            return f"Error: Failed to search contacts - {str(e)}"

    @mcp.tool()
    async def get_contact(identifier: str, include_all_fields: bool = True) -> str:
        """Get a contact by resource name or email with comprehensive information.

        Args:
            identifier: Resource name (people/*) or email address of the contact
            include_all_fields: Whether to include all contact fields (default: True)
        """
        service = init_service()
        if not service:
            return "Error: Google Contacts service is not available. Please check your credentials."

        try:
            contact = service.get_contact(identifier, include_all_fields)
            return format_contact(contact)
        except Exception as e:
            return f"Error: Failed to get contact - {str(e)}"

    @mcp.tool()
    async def create_contact(
        given_name: str,
        family_name: Optional[str] = None,
        email: Optional[str] = None,
        phone: Optional[str] = None,
        organization: Optional[str] = None,
        job_title: Optional[str] = None,
        address: Optional[str] = None,
        birthday: Optional[str] = None,
        website: Optional[str] = None,
        notes: Optional[str] = None,
        nickname: Optional[str] = None,
    ) -> str:
        """Create a new contact with comprehensive field support.

        Args:
            given_name: First name of the contact
            family_name: Last name of the contact
            email: Email address of the contact
            phone: Phone number of the contact
            organization: Company/organization name
            job_title: Job title or position
            address: Physical address
            birthday: Birthday in YYYY-MM-DD format
            website: Website URL
            notes: Notes or biography
            nickname: Nickname
        """
        service = init_service()
        if not service:
            return "Error: Google Contacts service is not available. Please check your credentials."

        try:
            contact_data = {"given_name": given_name}

            # Add optional fields if provided
            if family_name:
                contact_data["family_name"] = family_name
            if email:
                contact_data["email"] = email
            if phone:
                contact_data["phone"] = phone
            if organization:
                contact_data["organization"] = organization
            if job_title:
                contact_data["job_title"] = job_title
            if address:
                contact_data["address"] = address
            if birthday:
                contact_data["birthday"] = birthday
            if website:
                contact_data["website"] = website
            if notes:
                contact_data["notes"] = notes
            if nickname:
                contact_data["nickname"] = nickname

            contact = service.create_contact(contact_data)
            return f"Contact created successfully!\n\n{format_contact(contact)}"
        except Exception as e:
            return f"Error: Failed to create contact - {str(e)}"

    @mcp.tool()
    async def create_contact_advanced(contact_data: Dict[str, Any]) -> str:
        """Create a new contact with full field support including multiple emails, phones, addresses, etc.

        Args:
            contact_data: Dictionary containing complete contact information with support for:
                - Multiple emails: {"emails": [{"value": "email@example.com", "type": "work"}]}
                - Multiple phones: {"phones": [{"value": "+1234567890", "type": "mobile"}]}
                - Multiple addresses: {"addresses": [{"formatted": "123 Main St", "type": "home"}]}
                - Relations: {"relations": [{"person": "John Doe", "type": "spouse"}]}
                - Events: {"events": [{"date": {"month": 12, "day": 25}, "type": "anniversary"}]}
                - Custom fields: {"custom_fields": [{"key": "Department", "value": "Engineering"}]}
        """
        service = init_service()
        if not service:
            return "Error: Google Contacts service is not available. Please check your credentials."

        try:
            contact = service.create_contact(contact_data)
            return f"Advanced contact created successfully!\n\n{format_contact(contact)}"
        except Exception as e:
            return f"Error: Failed to create advanced contact - {str(e)}"

    @mcp.tool()
    async def update_contact(
        resource_name: str,
        given_name: Optional[str] = None,
        family_name: Optional[str] = None,
        email: Optional[str] = None,
        phone: Optional[str] = None,
        organization: Optional[str] = None,
        job_title: Optional[str] = None,
        address: Optional[str] = None,
        birthday: Optional[str] = None,
        website: Optional[str] = None,
        notes: Optional[str] = None,
        nickname: Optional[str] = None,
    ) -> str:
        """Update an existing contact with comprehensive field support.

        Args:
            resource_name: Contact resource name (people/*)
            given_name: Updated first name
            family_name: Updated last name
            email: Updated email address
            phone: Updated phone number
            organization: Updated company/organization name
            job_title: Updated job title or position
            address: Updated physical address
            birthday: Updated birthday in YYYY-MM-DD format
            website: Updated website URL
            notes: Updated notes or biography
            nickname: Updated nickname
        """
        service = init_service()
        if not service:
            return "Error: Google Contacts service is not available. Please check your credentials."

        try:
            contact_data = {}

            # Add fields that are being updated
            if given_name is not None:
                contact_data["given_name"] = given_name
            if family_name is not None:
                contact_data["family_name"] = family_name
            if email is not None:
                contact_data["email"] = email
            if phone is not None:
                contact_data["phone"] = phone
            if organization is not None:
                contact_data["organization"] = organization
            if job_title is not None:
                contact_data["job_title"] = job_title
            if address is not None:
                contact_data["address"] = address
            if birthday is not None:
                contact_data["birthday"] = birthday
            if website is not None:
                contact_data["website"] = website
            if notes is not None:
                contact_data["notes"] = notes
            if nickname is not None:
                contact_data["nickname"] = nickname

            if not contact_data:
                return "Error: No fields provided for update."

            contact = service.update_contact(resource_name, contact_data)
            return f"Contact updated successfully!\n\n{format_contact(contact)}"
        except Exception as e:
            return f"Error: Failed to update contact - {str(e)}"

    @mcp.tool()
    async def update_contact_advanced(resource_name: str, contact_data: Dict[str, Any]) -> str:
        """Update an existing contact with full field support including multiple emails, phones, addresses, etc.

        Args:
            resource_name: Contact resource name (people/*)
            contact_data: Dictionary containing updated contact information with full field support
        """
        service = init_service()
        if not service:
            return "Error: Google Contacts service is not available. Please check your credentials."

        try:
            contact = service.update_contact(resource_name, contact_data)
            return f"Advanced contact updated successfully!\n\n{format_contact(contact)}"
        except Exception as e:
            return f"Error: Failed to update advanced contact - {str(e)}"

    @mcp.tool()
    async def delete_contact(resource_name: str) -> str:
        """Delete a contact by resource name.

        Args:
            resource_name: Contact resource name (people/*) to delete
        """
        service = init_service()
        if not service:
            return "Error: Google Contacts service is not available. Please check your credentials."

        try:
            result = service.delete_contact(resource_name)
            if result.get("success"):
                return f"Contact {resource_name} deleted successfully."
            else:
                return f"Failed to delete contact: {result.get('message', 'Unknown error')}"
        except Exception as e:
            return f"Error: Failed to delete contact - {str(e)}"


def register_directory_tools(mcp: FastMCP) -> None:
    """Register directory and workspace tools with the MCP server."""

    @mcp.tool()
    async def list_workspace_users(query: Optional[str] = None, max_results: int = 50) -> str:
        """List Google Workspace users in your organization's directory.

        This tool allows you to search and list users in your Google Workspace directory,
        including their email addresses and other information.

        Args:
            query: Optional search term to find specific users (name, email, etc.)
            max_results: Maximum number of results to return (default: 50)
        """
        service = init_service()
        if not service:
            return "Error: Google Contacts service is not available. Please check your credentials."

        try:
            workspace_users = service.list_directory_people(query=query, max_results=max_results)
            return format_directory_people(workspace_users, query)
        except Exception as e:
            return f"Error: Failed to list Google Workspace users - {str(e)}"

    @mcp.tool()
    async def search_directory(query: str, max_results: int = 20) -> str:
        """Search for people specifically in the Google Workspace directory.

        This performs a more targeted search of your organization's directory.

        Args:
            query: Search term to find specific directory members
            max_results: Maximum number of results to return (default: 20)
        """
        service = init_service()
        if not service:
            return "Error: Google Contacts service is not available. Please check your credentials."

        try:
            results = service.search_directory(query, max_results)
            return format_directory_people(results, query)
        except Exception as e:
            return f"Error: Failed to search directory - {str(e)}"

    @mcp.tool()
    async def get_other_contacts(max_results: int = 50) -> str:
        """Retrieve contacts from the 'Other contacts' section.

        Other contacts are people you've interacted with but haven't added to your contacts list.
        These often include email correspondents that aren't in your main contacts.

        Args:
            max_results: Maximum number of results to return (default: 50)
        """
        service = init_service()
        if not service:
            return "Error: Google Contacts service is not available. Please check your credentials."

        try:
            other_contacts = service.get_other_contacts(max_results)

            if not other_contacts:
                return "No 'Other contacts' found in your Google account."

            # Count how many have email addresses
            with_email = sum(1 for c in other_contacts if c.get("email"))

            # Format and return the results
            formatted_list = format_contacts_list(other_contacts)
            return f"Other Contacts (people you've interacted with but haven't added):\n\n{formatted_list}\n\n{with_email} of these contacts have email addresses."
        except Exception as e:
            return f"Error: Failed to retrieve other contacts - {str(e)}"


def register_contact_group_tools(mcp: FastMCP) -> None:
    """Register contact group management tools with the MCP server."""

    @mcp.tool()
    async def list_contact_groups(include_system_groups: bool = True) -> str:
        """List all contact groups (labels) in your Google Contacts.

        Contact groups are like labels that help you organize your contacts into categories
        such as 'Family', 'Work', 'Friends', etc.

        Args:
            include_system_groups: Whether to include system groups like "My Contacts", "Starred", etc.
        """
        service = init_service()
        if not service:
            return "Error: Google Contacts service is not available. Please check your credentials."

        try:
            groups = service.list_contact_groups(include_system_groups)
            return format_contact_groups_list(groups)
        except Exception as e:
            return f"Error: Failed to list contact groups - {str(e)}"

    @mcp.tool()
    async def create_contact_group(name: str, client_data: List[Dict[str, str]] = None) -> str:
        """Create a new contact group (label) to organize your contacts.

        Args:
            name: Name for the new contact group (e.g., "Work Colleagues", "Family", "Book Club")
            client_data: Optional custom data as list of key-value pairs (e.g., [{"key": "color", "value": "blue"}])
        """
        service = init_service()
        if not service:
            return "Error: Google Contacts service is not available. Please check your credentials."

        try:
            group = service.create_contact_group(name, client_data)
            return f"Contact group created successfully!\n\n{format_contact_group(group)}"
        except Exception as e:
            return f"Error: Failed to create contact group - {str(e)}"

    @mcp.tool()
    async def get_contact_group(
        resource_name: str, include_members: bool = False, max_members: int = 50
    ) -> str:
        """Get detailed information about a specific contact group.

        Args:
            resource_name: Contact group resource name (e.g., "contactGroups/12345")
            include_members: Whether to include the list of member contact IDs
            max_members: Maximum number of member IDs to return if include_members is True
        """
        service = init_service()
        if not service:
            return "Error: Google Contacts service is not available. Please check your credentials."

        try:
            max_members_param = max_members if include_members else 0
            group = service.get_contact_group(resource_name, max_members_param)
            return format_contact_group(group)
        except Exception as e:
            return f"Error: Failed to get contact group - {str(e)}"

    @mcp.tool()
    async def update_contact_group(
        resource_name: str, name: str, client_data: List[Dict[str, str]] = None
    ) -> str:
        """Update a contact group's name and custom data.

        Args:
            resource_name: Contact group resource name (e.g., "contactGroups/12345")
            name: New name for the contact group
            client_data: Optional updated custom data as list of key-value pairs
        """
        service = init_service()
        if not service:
            return "Error: Google Contacts service is not available. Please check your credentials."

        try:
            group = service.update_contact_group(resource_name, name, client_data)
            return f"Contact group updated successfully!\n\n{format_contact_group(group)}"
        except Exception as e:
            return f"Error: Failed to update contact group - {str(e)}"

    @mcp.tool()
    async def delete_contact_group(resource_name: str) -> str:
        """Delete a contact group. Note: This only works for user-created groups, not system groups.

        Args:
            resource_name: Contact group resource name (e.g., "contactGroups/12345")
        """
        service = init_service()
        if not service:
            return "Error: Google Contacts service is not available. Please check your credentials."

        try:
            result = service.delete_contact_group(resource_name)
            if result.get("success"):
                return f"Contact group {resource_name} deleted successfully."
            else:
                return f"Failed to delete contact group: {result.get('message', 'Unknown error')}"
        except Exception as e:
            return f"Error: Failed to delete contact group - {str(e)}"

    @mcp.tool()
    async def add_contacts_to_group(
        group_resource_name: str, contact_resource_names: List[str]
    ) -> str:
        """Add contacts to a contact group (assign a label to contacts).

        Args:
            group_resource_name: Contact group resource name (e.g., "contactGroups/12345")
            contact_resource_names: List of contact resource names to add (e.g., ["people/12345", "people/67890"])
        """
        service = init_service()
        if not service:
            return "Error: Google Contacts service is not available. Please check your credentials."

        try:
            result = service.add_contacts_to_group(group_resource_name, contact_resource_names)
            return format_group_membership_result(result, "add")
        except Exception as e:
            return f"Error: Failed to add contacts to group - {str(e)}"

    @mcp.tool()
    async def remove_contacts_from_group(
        group_resource_name: str, contact_resource_names: List[str]
    ) -> str:
        """Remove contacts from a contact group (remove a label from contacts).

        Args:
            group_resource_name: Contact group resource name (e.g., "contactGroups/12345")
            contact_resource_names: List of contact resource names to remove (e.g., ["people/12345", "people/67890"])
        """
        service = init_service()
        if not service:
            return "Error: Google Contacts service is not available. Please check your credentials."

        try:
            result = service.remove_contacts_from_group(group_resource_name, contact_resource_names)
            return format_group_membership_result(result, "remove")
        except Exception as e:
            return f"Error: Failed to remove contacts from group - {str(e)}"

    @mcp.tool()
    async def search_contacts_by_group(group_resource_name: str, max_results: int = 50) -> str:
        """Find all contacts that belong to a specific contact group.

        This is useful for seeing which contacts have a particular label assigned.

        Args:
            group_resource_name: Contact group resource name (e.g., "contactGroups/12345")
            max_results: Maximum number of contacts to return
        """
        service = init_service()
        if not service:
            return "Error: Google Contacts service is not available. Please check your credentials."

        try:
            # Get the group with member resource names
            group = service.get_contact_group(group_resource_name, max_results)

            if not group.get("memberResourceNames"):
                return f"No contacts found in group '{group.get('name', 'Unknown Group')}'"

            # Get full contact details for each member
            member_contacts = []
            for member_resource_name in group["memberResourceNames"]:
                try:
                    contact = service.get_contact(member_resource_name, include_all_fields=False)
                    member_contacts.append(contact)
                except Exception:
                    # Skip contacts that can't be retrieved
                    continue

            if not member_contacts:
                return (
                    f"No accessible contacts found in group '{group.get('name', 'Unknown Group')}'"
                )

            group_name = group.get("name", "Unknown Group")
            return f"Contacts in group '{group_name}':\n\n{format_contacts_list(member_contacts)}"
        except Exception as e:
            return f"Error: Failed to search contacts by group - {str(e)}"
