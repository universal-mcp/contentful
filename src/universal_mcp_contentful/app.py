from typing import Any, Dict, List, Optional

from universal_mcp.applications import GraphQLApplication
from universal_mcp.integrations import Integration
from loguru import logger

class ContentfulApp(GraphQLApplication):
    def __init__(
        self, integration: Optional[Integration] = None, **kwargs: Any,
    ) -> None:
        """
        Initialize the Contentful application.

        Args:
            integration: Optional Integration configuration providing credentials.
                         Expected credentials:
                         - 'space_id' (str): The Contentful space ID.
                         - 'access_token' (str): The Content Delivery API or Content Preview API access token.
                         - 'api_key' (str, optional): Alternative for 'access_token'.
                         - 'environment_id' (str, optional): The environment ID (defaults to "master").
                         - 'is_eu_customer' (bool, optional): Set to True if using Contentful's EU data center (defaults to False).
            **kwargs: Additional keyword arguments passed to the base application.
        """
        # Initialize Contentful-specific attributes to defaults first.
        # These will be updated if an integration with valid credentials is provided.
        self.space_id: Optional[str] = None
        self.environment_id: str = "master"
        self._access_token: Optional[str] = None 
        
        # This variable will hold the base_url to be passed to the superclass constructor.
        # It will be a functional URL if configuration is correct, or a placeholder otherwise.
        calculated_base_url_for_super: str

        if not integration:
            logger.error(
                "Contentful integration not configured. Required credentials (space_id, access_token) "
                "are missing. API calls will likely fail."
            )
            # GraphQLApplication's __init__ expects a string for base_url.
            calculated_base_url_for_super = "http://mock.contentful/graphql/no-integration-provided"
        else:
            credentials = integration.get_credentials()
            
            # Set instance attributes from credentials. These are used to build the base_url.
            self.space_id = credentials.get("space_id")
            self._access_token = credentials.get("access_token") or credentials.get("api_key")
            self.environment_id = credentials.get("environment_id", "master")
            is_eu_customer = credentials.get("is_eu_customer", False)

            if not self.space_id:
                logger.error(
                    "Contentful 'space_id' not found in integration credentials. "
                    "Base URL cannot be constructed, and API calls will fail."
                )
                calculated_base_url_for_super = "http://mock.contentful/graphql/missing-space-id"
            else:
                # Construct the actual base_url since space_id is available
                contentful_api_domain = (
                    "graphql.eu.contentful.com"
                    if is_eu_customer
                    else "graphql.contentful.com"
                )
                calculated_base_url_for_super = f"https://{contentful_api_domain}/content/v1/spaces/{self.space_id}/environments/{self.environment_id}"
            
            if not self._access_token:
                logger.warning(
                    "Contentful 'access_token' or 'api_key' not found in integration credentials. "
                    "API calls may fail due to missing authentication."
                )
        
        # Call the superclass __init__ with the determined base_url.
        # GraphQLApplication's __init__ will store this as self.base_url.
        # It also stores the integration object as self.integration.
        super().__init__(name="contentful", base_url=calculated_base_url_for_super, integration=integration, **kwargs)

        # Instance attributes like self.space_id, self.environment_id, self._access_token are already set.
        # self.base_url and self.integration are set by the superclass constructor.

        logger.info(
            f"ContentfulApp initialized for space '{self.space_id if self.space_id else 'Not Set'}', "
            f"environment '{self.environment_id}'. "
            # self.base_url is guaranteed to be set by GraphQLApplication's __init__
            f"Base URL: {self.base_url}" 
        )
        
    @staticmethod
    def _to_camel_case(s: str) -> str:
        """Converts a string to camelCase based on Contentful's typical ID to GraphQL name conversion."""
        s = s.replace("-", " ").replace("_", " ")
        parts = s.split()
        if not parts:
            return ""
        # Contentful IDs are often like "my-content-type" or "myContentType"
        # If already somewhat camel/pascal, splitting might not be ideal.
        # Assuming input is a typical Contentful ID.
        # For "my-content-type": parts = ["my", "content", "type"] -> "myContentType"
        # For "myContentType": parts = ["myContentType"] -> "mycontenttype" (oops)
        # Let's refine: if no separators, assume it's close to target.
        if len(parts) == 1 and parts[0] == s and s: # No separators found
             return s[0].lower() + s[1:] if len(s) > 1 else s.lower()

        return parts[0].lower() + "".join(word.capitalize() for word in parts[1:])


    @staticmethod
    def _to_pascal_case(s: str) -> str:
        """Converts a string to PascalCase based on Contentful's typical ID to GraphQL name conversion."""
        s = s.replace("-", " ").replace("_", " ")
        parts = s.split()
        if not parts:
            return ""
        # For "myContentType": parts = ["myContentType"] -> "Mycontenttype" (oops)
        if len(parts) == 1 and parts[0] == s and s: # No separators found
            return s[0].upper() + s[1:] if len(s) > 1 else s.upper()

        return "".join(word.capitalize() for word in parts)

    def get_entry(
        self,
        content_type_id: str,
        entry_id: str,
        fields_to_select: str,
        locale: Optional[str] = None,
        preview: bool = False,
    ) -> Dict[str, Any]:
        """
        Fetches a single entry of a specified content type by its ID.

        Args:
            content_type_id: The ID of the Contentful content type (e.g., "blogPost", "articlePage").
                             This is used to derive the GraphQL query field name.
            entry_id: The ID of the entry to fetch.
            fields_to_select: A GraphQL string representing the fields to select for the entry.
                              Example: "sys { id } title description"
            locale: Optional locale code (e.g., "en-US") to fetch localized content.
            preview: Set to True to fetch preview (unpublished) content. Requires a Preview API token.

        Returns:
            A dictionary containing the fetched entry data.
        
        Note:
            Contentful may alter the generated query field name if the simple camelCased
            `content_type_id` conflicts with reserved names (e.g., 'location' becomes 'contentTypeLocation').
            If you encounter issues, verify the exact query field name in GraphiQL
            and consider using `execute_graphql_query` for such cases.
        """
        query_field = self._to_camel_case(content_type_id)
        logger.debug(
            f"Fetching entry for content_type_id='{content_type_id}' (query field='{query_field}'), entry_id='{entry_id}'"
        )

        query_gql = f"""
            query GetEntryById($id: String!, $locale: String, $preview: Boolean) {{
                {query_field}(id: $id, locale: $locale, preview: $preview) {{
                    {fields_to_select}
                }}
            }}
        """
        variables: Dict[str, Any] = {"id": entry_id, "preview": preview}
        if locale:
            variables["locale"] = locale
        
        return self.query(query_gql, variables=variables)

    def get_entries_collection(
        self,
        content_type_id: str,
        fields_to_select_for_item: str,
        limit: Optional[int] = None,
        skip: Optional[int] = None,
        where: Optional[Dict[str, Any]] = None,
        order: Optional[List[str]] = None,
        locale: Optional[str] = None,
        preview: bool = False,
    ) -> Dict[str, Any]:
        """
        Fetches a collection of entries of a specified content type.

        Args:
            content_type_id: The ID of the Contentful content type (e.g., "blogPost").
            fields_to_select_for_item: GraphQL string for fields to select on each item in the collection.
                                       Example: "sys { id } title summary"
            limit: Maximum number of items to return.
            skip: Number of items to skip for pagination.
            where: Filter object for the query (e.g., {"title_contains": "Hello"}).
                   The structure depends on the generated GQL Filter type for the content type.
            order: List of order specifications (e.g., ["sys_firstPublishedAt_DESC", "title_ASC"]).
                   The items depend on the generated GQL Order enum for the content type.
            locale: Optional locale code.
            preview: Set to True to fetch preview content.

        Returns:
            A dictionary containing the collection data (total, skip, limit, items).

        Note:
            Similar to `get_entry`, the derived GraphQL field names and type names for filters/orders
            might need adjustment for Contentful's collision handling.
            The types for $where and $order variables are `ContentTypeFilter` and `[ContentTypeOrder!]` respectively.
        """
        collection_field = self._to_camel_case(content_type_id) + "Collection"
        filter_type = self._to_pascal_case(content_type_id) + "Filter"
        order_enum_type = self._to_pascal_case(content_type_id) + "Order"
        logger.debug(
            f"Fetching collection for content_type_id='{content_type_id}' "
            f"(collection field='{collection_field}', filter type='{filter_type}', order enum='{order_enum_type}')"
        )

        query_gql = f"""
            query GetEntries(
                $limit: Int,
                $skip: Int,
                $where: {filter_type},
                $order: [{order_enum_type}!],
                $locale: String,
                $preview: Boolean
            ) {{
                {collection_field}(
                    limit: $limit,
                    skip: $skip,
                    where: $where,
                    order: $order,
                    locale: $locale,
                    preview: $preview
                ) {{
                    total
                    skip
                    limit
                    items {{
                        {fields_to_select_for_item}
                    }}
                }}
            }}
        """
        variables: Dict[str, Any] = {"preview": preview}
        if limit is not None:
            variables["limit"] = limit
        if skip is not None:
            variables["skip"] = skip
        if where:
            variables["where"] = where
        if order:
            variables["order"] = order
        if locale:
            variables["locale"] = locale
            
        return self.query(query_gql, variables=variables)

    def get_asset(
        self,
        asset_id: str,
        fields_to_select: str = "sys { id } url title description fileName contentType size width height",
        preview: bool = False,
    ) -> Dict[str, Any]:
        """
        Fetches a single asset by its ID.

        Args:
            asset_id: The ID of the asset to fetch.
            fields_to_select: GraphQL string for fields to select on the asset.
                              Includes common fields by default. For image transforms, query `url(transform: { ... })`.
            preview: Set to True to fetch preview (unpublished) asset data.

        Returns:
            A dictionary containing the fetched asset data.
        """
        logger.debug(f"Fetching asset_id='{asset_id}'")
        query_gql = f"""
            query GetAssetById($id: String!, $preview: Boolean) {{
                asset(id: $id, preview: $preview) {{
                    {fields_to_select}
                }}
            }}
        """
        variables: Dict[str, Any] = {"id": asset_id, "preview": preview}
        return self.query(query_gql, variables=variables)

    def get_assets_collection(
        self,
        fields_to_select_for_item: str = "sys { id } url title description fileName contentType size width height",
        limit: Optional[int] = None,
        skip: Optional[int] = None,
        where: Optional[Dict[str, Any]] = None,
        order: Optional[List[str]] = None,
        locale: Optional[str] = None,
        preview: bool = False,
    ) -> Dict[str, Any]:
        """
        Fetches a collection of assets.

        Args:
            fields_to_select_for_item: GraphQL string for fields to select on each asset item.
            limit: Maximum number of items to return.
            skip: Number of items to skip for pagination.
            where: Filter object for the query (AssetFilter structure).
            order: List of order specifications (AssetOrder enum values).
            locale: Optional locale code (for localized asset fields like title, description).
            preview: Set to True to fetch preview content.

        Returns:
            A dictionary containing the asset collection data.
        """
        logger.debug("Fetching assets collection")
        # AssetFilter and AssetOrder are standard Contentful types
        query_gql = f"""
            query GetAssets(
                $limit: Int,
                $skip: Int,
                $where: AssetFilter,
                $order: [AssetOrder!],
                $locale: String,
                $preview: Boolean
            ) {{
                assetCollection(
                    limit: $limit,
                    skip: $skip,
                    where: $where,
                    order: $order,
                    locale: $locale,
                    preview: $preview
                ) {{
                    total
                    skip
                    limit
                    items {{
                        {fields_to_select_for_item}
                    }}
                }}
            }}
        """
        variables: Dict[str, Any] = {"preview": preview}
        if limit is not None:
            variables["limit"] = limit
        if skip is not None:
            variables["skip"] = skip
        if where:
            variables["where"] = where
        if order:
            variables["order"] = order
        if locale:
            variables["locale"] = locale

        return self.query(query_gql, variables=variables)

    def execute_graphql_query(
        self, query_string: str, variables: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Executes an arbitrary GraphQL query.

        Args:
            query_string: The GraphQL query string.
            variables: Optional dictionary of variables for the query.

        Returns:
            The result of the query.
        """
        logger.debug(f"Executing custom GraphQL query with variables: {variables}")
        return self.query(query_string, variables=variables)

    def execute_graphql_mutation(
        self, mutation_string: str, variables: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Executes an arbitrary GraphQL mutation.
        Note: Contentful's GraphQL Content API is read-only. This method is
              provided for general GraphQLApplication compatibility.

        Args:
            mutation_string: The GraphQL mutation string.
            variables: Optional dictionary of variables for the mutation.

        Returns:
            The result of the mutation.
        """
        logger.debug(f"Executing custom GraphQL mutation with variables: {variables}")
        return self.mutate(mutation_string, variables=variables)

    def list_tools(self) -> List[callable]:
        return [
            self.get_entry,
            self.get_entries_collection,
            self.get_asset,
            self.get_assets_collection,
            self.execute_graphql_query,
            self.execute_graphql_mutation,
        ]