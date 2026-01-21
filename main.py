#!/usr/bin/env python3
"""
Monday.com Attachment Downloader Toolkit

Downloads attachments from Monday.com boards with parallel processing,
group filtering, and status-based item selection.
"""

import json
import sys
import os
import re
import asyncio
import aiohttp
import aiofiles
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Tuple, Callable, Any


# ============================================================================
# Configuration Class
# ============================================================================

class MondayConfig:
    """Configuration holder for Monday.com API settings"""
    
    def __init__(self, args: dict):
        self.api_url = "https://api.monday.com/v2"
        self.api_token = args.get("api_token", "")
        self.workspace_name = args.get("workspace_name", "")
        self.board_name = args.get("board_name", "")
        
        # Parse groups - ensure it's a list, not a string
        groups_input = args.get("groups", [])
        if isinstance(groups_input, str):
            # Try to parse as JSON if it's a string
            try:
                import json as json_parser
                self.groups = json_parser.loads(groups_input)
            except (json.JSONDecodeError, ValueError):
                # If it looks like a comma-separated list, split it
                if ',' in groups_input:
                    self.groups = [g.strip().strip('"\'') for g in groups_input.split(',')]
                else:
                    self.groups = [groups_input] if groups_input else []
        elif isinstance(groups_input, list):
            self.groups = groups_input
        else:
            self.groups = []
        
        # Parse group_folder_map - ensure it's a dict, not a string
        folder_map_input = args.get("group_folder_map", {})
        if isinstance(folder_map_input, str):
            try:
                import json as json_parser
                self.group_folder_map = json_parser.loads(folder_map_input)
            except (json.JSONDecodeError, ValueError):
                self.group_folder_map = {}
        elif isinstance(folder_map_input, dict):
            self.group_folder_map = folder_map_input
        else:
            self.group_folder_map = {}
        
        # Column titles
        self.status_column_title = args.get("status_column_title", "Status")
        self.date_column_title = args.get("date_column_title", "Date")
        
        # Status values
        self.target_status = args.get("target_status", "Retry")
        self.new_status = args.get("new_status", "In Queue")
        # Note: The exact label from Monday.com board - with two spaces before (ERROR)
        self.error_status = args.get("error_status", "PO not FOUND  (ERROR)")
        
        # Flags
        self.debug_mode = args.get("debug_mode", False)
        
        # Cache
        self._status_column_id = None
        self._date_column_id = None
        self._group_id_cache: Dict[str, str] = {}
        self._board_id = None


# ============================================================================
# HTTP Client
# ============================================================================

class MondayHttpClient:
    """HTTP client for Monday.com API calls"""
    
    def __init__(self, config: MondayConfig):
        self.config = config
        self._session: Optional[aiohttp.ClientSession] = None
        self._download_session: Optional[aiohttp.ClientSession] = None
    
    async def get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={
                    "Authorization": self.config.api_token,
                    "Content-Type": "application/json"
                }
            )
        return self._session
    
    async def get_download_session(self) -> aiohttp.ClientSession:
        """Get a session with larger header limits for file downloads (no default auth)"""
        if self._download_session is None or self._download_session.closed:
            # Create connector with larger header size limit (64KB instead of 8KB default)
            # NOTE: Do NOT set Authorization header here - public URLs (S3/CDN) don't need it
            # and may reject requests with unexpected auth headers
            connector = aiohttp.TCPConnector(limit=100)
            self._download_session = aiohttp.ClientSession(
                connector=connector,
                max_line_size=65536,  # 64KB max header line size
                max_field_size=65536  # 64KB max header field size
            )
        return self._download_session
    
    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
        if self._download_session and not self._download_session.closed:
            await self._download_session.close()
    
    async def post(self, payload: dict, max_retries: int = 3) -> dict:
        """Make a POST request to Monday.com GraphQL API with retry logic for rate limits"""
        session = await self.get_session()
        
        for attempt in range(max_retries + 1):
            try:
                async with session.post(self.config.api_url, json=payload) as response:
                    if response.status == 429:
                        # Rate limited - extract retry_in_seconds from response
                        error_body = await response.text()
                        retry_seconds = 35  # Default wait time
                        
                        # Try to parse the retry time from response
                        try:
                            import re
                            match = re.search(r'"retry_in_seconds":\s*(\d+)', error_body)
                            if match:
                                retry_seconds = int(match.group(1))
                        except:
                            pass
                        
                        if attempt < max_retries:
                            print(f"Rate limited (429). Waiting {retry_seconds} seconds before retry {attempt + 1}/{max_retries}...")
                            await asyncio.sleep(retry_seconds)
                            continue
                        else:
                            raise Exception(f"HTTP error 429 after {max_retries} retries: {error_body}")
                    
                    if not response.ok:
                        error_body = await response.text()
                        raise Exception(f"HTTP error {response.status}: {error_body}")
                    
                    return await response.json()
                    
            except aiohttp.ClientError as e:
                if attempt < max_retries:
                    wait_time = 2 ** attempt  # Exponential backoff: 1, 2, 4 seconds
                    print(f"Network error: {e}. Retrying in {wait_time} seconds ({attempt + 1}/{max_retries})...")
                    await asyncio.sleep(wait_time)
                    continue
                raise Exception(f"Network error after {max_retries} retries: {e}")
        
        raise Exception("Unexpected error in post method")
    
    async def download_file(self, url: str, use_auth: bool = False) -> bytes:
        """
        Download file from URL - uses session with larger header limits
        
        Args:
            url: The URL to download from
            use_auth: If True, include Authorization header
        """
        session = await self.get_download_session()
        
        # Prepare headers - optionally include auth
        headers = {}
        if use_auth:
            headers["Authorization"] = self.config.api_token
        
        try:
            async with session.get(url, headers=headers if headers else None) as response:
                if not response.ok:
                    # Read first bit of error body for debugging
                    error_preview = await response.text()
                    error_preview = error_preview[:200] if len(error_preview) > 200 else error_preview
                    raise Exception(f"HTTP {response.status}: {error_preview}")
                
                data = await response.read()
                
                # Validate that we got actual file content, not an HTML error page
                # Check first few bytes for HTML signature
                if len(data) > 15 and (data[:15].lower().startswith(b'<!doctype html') or data[:6].lower().startswith(b'<html')):
                    raise Exception(f"Response is HTML, not file content (size: {len(data)} bytes)")
                
                # Also check for very small files that might be error responses
                if len(data) < 100:
                    # Check if it looks like an error message
                    try:
                        text = data.decode('utf-8', errors='ignore')
                        if 'error' in text.lower() or 'not found' in text.lower():
                            raise Exception(f"Error response: {text}")
                    except UnicodeDecodeError:
                        pass  # Binary data is fine
                
                return data
        except aiohttp.ClientError as e:
            raise Exception(f"Network error: {type(e).__name__}: {e}")
    
    async def download_file_with_retry(self, url: str) -> bytes:
        """
        Download file with retry logic - tries without auth first, then with auth
        """
        last_error = None
        
        # Try 1: Download without auth
        try:
            return await self.download_file(url, use_auth=False)
        except Exception as e:
            last_error = e
        
        # Try 2: Download with auth (some URLs may require it)
        try:
            return await self.download_file(url, use_auth=True)
        except Exception as e:
            last_error = e
        
        raise last_error
    
    async def download_file_with_auth(self, url: str) -> bytes:
        """Download file with Monday.com API auth - uses session with larger header limits"""
        return await self.download_file(url, use_auth=True)


# ============================================================================
# File Downloader
# ============================================================================

class MondayFileDownloader:
    """Handles saving downloaded files to disk"""
    
    @staticmethod
    async def save(file_name: str, data: bytes, download_dir: str) -> str:
        """Save file to specified directory"""
        if not file_name or not file_name.strip():
            file_name = f"attachment_{int(datetime.now().timestamp() * 1000)}"
        
        dir_path = Path(download_dir)
        dir_path.mkdir(parents=True, exist_ok=True)
        
        # Handle duplicate filenames
        output_path = dir_path / file_name
        counter = 1
        base_name = file_name
        extension = ""
        
        last_dot = file_name.rfind('.')
        if last_dot > 0:
            base_name = file_name[:last_dot]
            extension = file_name[last_dot:]
        
        while output_path.exists():
            new_name = f"{base_name}_{counter}{extension}"
            output_path = dir_path / new_name
            counter += 1
        
        async with aiofiles.open(output_path, 'wb') as f:
            await f.write(data)
        
        return str(output_path)


# ============================================================================
# Item Service
# ============================================================================

class MondayItemService:
    """Service for managing Monday.com items"""
    
    def __init__(self, config: MondayConfig, http_client: MondayHttpClient):
        self.config = config
        self.http = http_client
    
    async def get_status_column_id(self, board_id: int) -> str:
        """Get the status column ID from the board structure"""
        if self.config._status_column_id:
            return self.config._status_column_id
        
        payload = {
            "query": f"""query {{
                boards(ids: {board_id}) {{
                    columns {{ id title type }}
                }}
            }}"""
        }
        
        response = await self.http.post(payload)
        
        if "errors" in response:
            raise Exception(f"Failed to fetch columns: {response['errors']}")
        
        boards = response.get("data", {}).get("boards", [])
        if not boards:
            raise Exception(f"Board not found: {board_id}")
        
        columns = boards[0].get("columns", [])
        
        for col in columns:
            title = col.get("title", "")
            if self.config.status_column_title.lower() == title.lower():
                self.config._status_column_id = col.get("id")
                print(f"Found Status column ID: {self.config._status_column_id}")
            if self.config.date_column_title.lower() == title.lower():
                self.config._date_column_id = col.get("id")
                print(f"Found Date column ID: {self.config._date_column_id}")
        
        if not self.config._status_column_id:
            # Fallback: try to find by ID
            for col in columns:
                col_id = col.get("id", "")
                if col_id.lower() == "status" or "status" in col_id.lower():
                    self.config._status_column_id = col_id
                    print(f"Found Status column by ID: {self.config._status_column_id}")
                    break
        
        if not self.config._status_column_id:
            raise Exception(f"Status column not found in board {board_id}")
        
        return self.config._status_column_id
    
    async def get_date_column_id(self, board_id: int) -> Optional[str]:
        """Get the date column ID from the board structure"""
        if self.config._date_column_id:
            return self.config._date_column_id
        # Initialize both columns
        await self.get_status_column_id(board_id)
        return self.config._date_column_id
    
    async def initialize_group_cache(self, board_id: int) -> None:
        """Initialize group ID cache - fetch all groups once"""
        if self.config._group_id_cache:
            return
        
        print("Initializing group ID cache...")
        
        payload = {
            "query": f"""query {{
                boards(ids: {board_id}) {{
                    groups {{ id title }}
                }}
            }}"""
        }
        
        response = await self.http.post(payload)
        
        if "errors" in response:
            print(f"ERROR: Failed to fetch groups: {response['errors']}")
            return
        
        boards = response.get("data", {}).get("boards", [])
        if not boards:
            return
        
        groups = boards[0].get("groups", [])
        
        for group in groups:
            current_title = group.get("title", "").strip()
            group_id = group.get("id", "")
            
            self.config._group_id_cache[current_title] = group_id
            if current_title.startswith("> "):
                self.config._group_id_cache[current_title[2:].strip()] = group_id
        
        print(f"Cached {len(self.config._group_id_cache)} group IDs")
    
    def _get_group_id_by_title(self, group_title: str) -> Optional[str]:
        """Get group ID by group title (uses cache)"""
        cached_id = self.config._group_id_cache.get(group_title)
        if cached_id:
            return cached_id
        
        for title, gid in self.config._group_id_cache.items():
            if self._matches_group_title(group_title, title):
                return gid
        
        return None
    
    def _matches_group_title(self, target: str, current: str) -> bool:
        """Check if two group titles match (flexible matching)"""
        if target == current:
            return True
        
        if current.startswith("> "):
            current = current[2:].strip()
            if target == current:
                return True
        
        return self._matches_group_flexible(target, current)
    
    def _matches_group_flexible(self, target_group: str, current_group: str) -> bool:
        """Flexible group matching using keywords"""
        if "NPOP" in target_group and "NPOP" in current_group:
            target_has_la3 = "LA3" in target_group
            target_has_la6 = "LA6" in target_group
            current_has_la3 = "LA3" in current_group
            current_has_la6 = "LA6" in current_group
            
            if (target_has_la3 and current_has_la3) or (target_has_la6 and current_has_la6):
                if "SOBEYSMIF" in target_group and "SOBEYSMIF" in current_group:
                    return True
                if "MIFLAOPS" in target_group and "MIFLAOPS" in current_group:
                    return True
        
        if "New Tender" in target_group and "New Tender" in current_group:
            regions = ["Atlantic", "West", "Quebec", "Ontario"]
            for region in regions:
                if region in target_group and region in current_group:
                    return True
        
        if ("Pepsi" in target_group and "Pepsi" in current_group and
                "Load Tender" in target_group and "Load Tender" in current_group):
            return True
        
        return False
    
    def _extract_date_from_column(self, col: dict) -> str:
        """Extract date from a date column value"""
        try:
            value_json = col.get("value", "")
            if not value_json or value_json == "null":
                return ""
            
            value_node = json.loads(value_json)
            date = value_node.get("date", "")
            
            if date and len(date) >= 10:
                return date[:10]  # Extract YYYY-MM-DD
        except Exception:
            pass
        return ""
    
    async def stream_item_ids_from_group(
        self,
        board_id: int,
        group_title: str,
        limit: int,
        status_col_id: str,
        target_status: str,
        callback: Callable[[List[int]], None]
    ) -> None:
        """
        STREAMING VERSION: Get item IDs with callback for immediate processing
        Allows parallel downloading while fetching continues
        """
        group_id = self._get_group_id_by_title(group_title)
        if group_id:
            await self._stream_items_from_group_id_optimized(
                board_id, group_id, limit, status_col_id, target_status, callback
            )
        else:
            # Fallback: use non-streaming version
            items = await self._get_item_ids_from_group_by_search(
                board_id, group_title, limit, status_col_id, target_status
            )
            if items:
                callback(items)
    
    async def _stream_items_from_group_id_optimized(
        self,
        board_id: int,
        group_id: str,
        limit: int,
        status_col_id: str,
        target_status: str,
        callback: Callable[[List[int]], None]
    ) -> None:
        """STREAMING OPTIMIZED: Get items from group and call callback immediately for each batch"""
        cursor = None
        items_found = 0
        consecutive_empty_pages = 0
        
        date_col_id = None
        try:
            date_col_id = await self.get_date_column_id(board_id)
        except Exception as e:
            print(f"WARNING: Could not get date column ID: {e}")
        
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        date_format = "%Y-%m-%d"
        
        while items_found < limit:
            cursor_part = f', cursor: "{cursor}"' if cursor else ""
            page_limit = 100
            
            start_time = datetime.now()
            
            column_values_part = "column_values { id text value }" if status_col_id else ""
            
            query = f"""query {{
                boards(ids: {board_id}) {{
                    items_page(limit: {page_limit}{cursor_part}) {{
                        cursor
                        items {{
                            id
                            name
                            group {{ id title }}
                            {column_values_part}
                        }}
                    }}
                }}
            }}"""
            
            payload = {"query": query}
            response = await self.http.post(payload)
            
            if "errors" in response:
                items = await self._get_item_ids_from_group_id_alternative(
                    board_id, group_id, limit, status_col_id, target_status
                )
                if items:
                    callback(items)
                return
            
            boards = response.get("data", {}).get("boards", [])
            if not boards:
                break
            
            items_page = boards[0].get("items_page", {})
            items_node = items_page.get("items", [])
            
            if not items_node:
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= 2:
                    break
            else:
                consecutive_empty_pages = 0
            
            batch_items = []
            for item in items_node:
                if items_found >= limit:
                    break
                
                group_node = item.get("group")
                if not group_node:
                    continue
                
                item_group_id = group_node.get("id", "")
                if group_id != item_group_id:
                    continue
                
                if status_col_id:
                    columns_node = item.get("column_values", [])
                    if not columns_node:
                        if not target_status:
                            batch_items.append(int(item.get("id")))
                            items_found += 1
                        continue
                    
                    current_status = ""
                    item_date = ""
                    
                    for col in columns_node:
                        col_id = col.get("id", "")
                        if status_col_id == col_id:
                            current_status = col.get("text", "").strip()
                        if date_col_id and date_col_id == col_id:
                            item_date = self._extract_date_from_column(col)
                    
                    should_include = False
                    
                    if self.config.target_status.lower() == current_status.lower():
                        should_include = True
                    elif not current_status:
                        if date_col_id and item_date:
                            if item_date == today.strftime(date_format) or item_date == yesterday.strftime(date_format):
                                should_include = True
                        else:
                            should_include = True
                    
                    if should_include:
                        batch_items.append(int(item.get("id")))
                        items_found += 1
                else:
                    batch_items.append(int(item.get("id")))
                    items_found += 1
            
            query_time = (datetime.now() - start_time).total_seconds() * 1000
            if batch_items:
                print(f"    Found {len(batch_items)} matching item(s) in this page (total so far: {items_found}) - Query took {query_time:.0f}ms")
                # Call callback immediately with this batch for parallel processing
                callback(batch_items)
            
            cursor = items_page.get("cursor")
            if items_found >= limit or not cursor:
                break
    
    async def _get_item_ids_from_group_by_search(
        self,
        board_id: int,
        group_title: str,
        limit: int,
        status_col_id: Optional[str],
        target_status: Optional[str]
    ) -> List[int]:
        """Get the first N item IDs from a specific group by searching through items (fallback method)"""
        result = []
        cursor = None
        items_checked = 0
        
        while True:
            cursor_part = f', cursor: "{cursor}"' if cursor else ""
            column_values_part = "column_values { id text }" if status_col_id else ""
            
            query = f"""query {{
                boards(ids: {board_id}) {{
                    items_page(limit: 100{cursor_part}) {{
                        cursor
                        items {{
                            id
                            name
                            group {{ id title }}
                            {column_values_part}
                        }}
                    }}
                }}
            }}"""
            
            payload = {"query": query}
            response = await self.http.post(payload)
            
            if "errors" in response:
                print(f"ERROR: GraphQL errors: {response['errors']}")
                return result
            
            boards = response.get("data", {}).get("boards", [])
            if not boards:
                break
            
            items_page = boards[0].get("items_page", {})
            items_node = items_page.get("items", [])
            
            if not items_node:
                break
            
            for item in items_node:
                items_checked += 1
                group_node = item.get("group")
                if not group_node:
                    continue
                
                current_group_title = group_node.get("title", "").strip()
                
                if current_group_title.startswith("> "):
                    current_group_title = current_group_title[2:].strip()
                
                if len(result) >= limit:
                    break
                
                group_matches = self._matches_group(group_title, current_group_title)
                
                if not group_matches:
                    continue
                
                if status_col_id:
                    columns_node = item.get("column_values", [])
                    if not columns_node:
                        if not target_status:
                            if not result:
                                print(f"   Found match for group: {group_title} (status: empty)")
                            result.append(int(item.get("id")))
                        continue
                    
                    current_status = ""
                    for col in columns_node:
                        col_id = col.get("id", "")
                        if status_col_id == col_id:
                            current_status = col.get("text", "").strip()
                            break
                    
                    if not current_status or (target_status and target_status.lower() == current_status.lower()):
                        if not result:
                            print(f"   Found match for group: {group_title} (status: '{current_status}')")
                        result.append(int(item.get("id")))
                else:
                    if not result:
                        print(f"   Found match for group: {group_title}")
                    result.append(int(item.get("id")))
            
            cursor = items_page.get("cursor")
            if not cursor:
                break
        
        if not result:
            print(f"   WARNING: Searched {items_checked} items but didn't find group: {group_title}")
        else:
            print(f"   Found {len(result)} item(s) in group: {group_title}")
        
        return result
    
    async def _get_item_ids_from_group_id_alternative(
        self,
        board_id: int,
        group_id: str,
        limit: int,
        status_col_id: Optional[str],
        target_status: Optional[str]
    ) -> List[int]:
        """Alternative method to get first N items from group with status filtering"""
        result = []
        cursor = None
        
        while len(result) < limit:
            cursor_part = f', cursor: "{cursor}"' if cursor else ""
            
            query = f"""query {{
                boards(ids: {board_id}) {{
                    items_page(limit: 100{cursor_part}) {{
                        cursor
                        items {{
                            id
                            group {{ id title }}
                        }}
                    }}
                }}
            }}"""
            
            payload = {"query": query}
            response = await self.http.post(payload)
            
            if "errors" in response:
                break
            
            boards = response.get("data", {}).get("boards", [])
            if not boards:
                break
            
            items_page = boards[0].get("items_page", {})
            items_node = items_page.get("items", [])
            
            if items_node:
                for item in items_node:
                    if len(result) >= limit:
                        break
                    
                    group_node = item.get("group")
                    if not group_node:
                        continue
                    
                    item_group_id = group_node.get("id", "")
                    if group_id != item_group_id:
                        continue
                    
                    result.append(int(item.get("id")))
            
            cursor = items_page.get("cursor")
            if len(result) >= limit or not cursor:
                break
        
        return result
    
    def _matches_group(self, target_group: str, current_group: str) -> bool:
        """Match group with all flexible matching logic"""
        if current_group == target_group:
            return True
        
        # Remove parenthetical content for comparison
        normalized_current = re.sub(r'\([^)]*\)', '', current_group).strip()
        normalized_target = re.sub(r'\([^)]*\)', '', target_group).strip()
        
        if normalized_current.lower() == normalized_target.lower():
            return True
        
        if "NPOP" in target_group and "NPOP" in current_group:
            target_key = self._extract_key_identifier(target_group)
            current_key = self._extract_key_identifier(current_group)
            if target_key and current_key and target_key == current_key:
                return True
        
        if "New Tender" in target_group and "New Tender" in current_group:
            target_region = self._extract_region_from_tender(target_group)
            current_region = self._extract_region_from_tender(current_group)
            if target_region and current_region and target_region == current_region:
                return True
        
        return self._matches_group_flexible(target_group, current_group)
    
    def _extract_key_identifier(self, group_title: str) -> Optional[str]:
        """Extract key identifier from group title"""
        start = group_title.find('(')
        end = group_title.find(')')
        if start >= 0 and end > start:
            return group_title[start + 1:end].strip()
        return None
    
    def _extract_region_from_tender(self, group_title: str) -> Optional[str]:
        """Extract region from 'New Tender' group title"""
        last_paren = group_title.rfind('(')
        last_close_paren = group_title.rfind(')')
        if last_paren >= 0 and last_close_paren > last_paren:
            return group_title[last_paren + 1:last_close_paren].strip()
        return None
    
    async def update_status(self, item_id: int, new_status: str, board_id: int) -> None:
        """Update Status column"""
        status_col_id = await self.get_status_column_id(board_id)
        
        query = f"""mutation {{
            change_simple_column_value(
                board_id: {board_id},
                item_id: {item_id},
                column_id: "{status_col_id}",
                value: "{new_status}"
            ) {{ id }}
        }}"""
        
        payload = {"query": query}
        response = await self.http.post(payload)
        
        if "errors" in response:
            print(f"ERROR: Failed to update status for item {item_id}")
            print(f"Errors: {response['errors']}")
            raise Exception(f"Status update failed: {response['errors']}")
        
        print(f"Status updated to '{new_status}' for item {item_id}")
    
    async def get_item_email(self, item_id: int, board_id: int) -> str:
        """Get item email from Email column"""
        # First, get the column definitions to find email column
        columns_query = f"""query {{
            boards(ids: {board_id}) {{
                columns {{ id title type }}
            }}
        }}"""
        
        columns_response = await self.http.post({"query": columns_query})
        
        # Build a map of column id -> column info
        column_map = {}
        if "data" in columns_response:
            boards = columns_response.get("data", {}).get("boards", [])
            if boards:
                for col in boards[0].get("columns", []):
                    column_map[col.get("id")] = {
                        "title": col.get("title", ""),
                        "type": col.get("type", "")
                    }
        
        # Now get the item's column values
        query = f"""query {{
            items(ids: [{item_id}]) {{
                id
                name
                column_values {{ id text value }}
            }}
        }}"""
        
        payload = {"query": query}
        response = await self.http.post(payload)
        
        if "errors" in response:
            print(f"WARNING: GraphQL errors while fetching email for item {item_id}")
            print(response['errors'])
            return "unknown"
        
        items_node = response.get("data", {}).get("items", [])
        if not items_node:
            print(f"WARNING: No items returned for item ID: {item_id}")
            return "unknown"
        
        item = items_node[0]
        columns = item.get("column_values", [])
        
        if not columns:
            print(f"WARNING: No column_values for item {item_id}")
            return "unknown"
        
        # Debug output
        if self.config.debug_mode:
            print(f"DEBUG: Columns for item {item_id}:")
            for col in columns:
                col_info = column_map.get(col.get("id"), {})
                print(f"  - ID: '{col.get('id')}' | Title: '{col_info.get('title')}' | Type: '{col_info.get('type')}' | Text: '{col.get('text')}'")
        
        # Strategy 1: Look for exact "Email" column (case-insensitive) using column map
        for col in columns:
            col_info = column_map.get(col.get("id"), {})
            title = col_info.get("title", "")
            if title.lower() == "email":
                email = col.get("text", "")
                if email and email != "null":
                    return self._sanitize_for_filename(email)
        
        # Strategy 2: Look for any column with "email" in the title
        for col in columns:
            col_info = column_map.get(col.get("id"), {})
            title = col_info.get("title", "").lower()
            if "email" in title:
                email = col.get("text", "")
                if email and email != "null":
                    return self._sanitize_for_filename(email)
        
        # Strategy 3: Look for email-type columns
        for col in columns:
            col_info = column_map.get(col.get("id"), {})
            col_type = col_info.get("type", "")
            if col_type == "email" or "email" in col_type:
                email = col.get("text", "")
                if email and email != "null":
                    return self._sanitize_for_filename(email)
        
        # Strategy 4: Parse value JSON for email columns
        for col in columns:
            col_info = column_map.get(col.get("id"), {})
            title = col_info.get("title", "").lower()
            col_type = col_info.get("type", "")
            
            if "email" in title or "email" in col_type:
                value_json = col.get("value", "")
                if value_json and value_json != "null":
                    try:
                        value_node = json.loads(value_json)
                        
                        if isinstance(value_node, dict):
                            if "email" in value_node:
                                email = value_node.get("email", "")
                                if email:
                                    return self._sanitize_for_filename(email)
                            
                            if "text" in value_node:
                                email = value_node.get("text", "")
                                if email:
                                    return self._sanitize_for_filename(email)
                        
                        if isinstance(value_node, str):
                            if value_node:
                                return self._sanitize_for_filename(value_node)
                    except Exception:
                        if "@" in value_json:
                            return self._sanitize_for_filename(value_json)
        
        # Strategy 5: Look for any column with @ in the text (email pattern)
        for col in columns:
            text = col.get("text", "")
            if text and "@" in text and "." in text:
                return self._sanitize_for_filename(text)
        
        return "unknown"
    
    def _sanitize_for_filename(self, text: str) -> str:
        """Sanitize text for use in filename"""
        if not text:
            return "unknown"
        return re.sub(r'[<>:"|?*\\/]', '_', text).strip()


# ============================================================================
# Attachment Service
# ============================================================================

class MondayAttachmentService:
    """Service for downloading attachments from Monday.com"""
    
    def __init__(self, config: MondayConfig, http_client: MondayHttpClient, item_service: MondayItemService):
        self.config = config
        self.http = http_client
        self.item_service = item_service
    
    async def download_attachments(
        self,
        item_id: int,
        download_dir: str,
        board_id: int,
        group_name: str
    ) -> Tuple[bool, int]:
        """
        Downloads all attachments for an item to specified directory.
        Returns (has_pdf, pdf_count)
        
        This method queries both:
        1. Item-level assets (from Files column)
        2. Update-level assets (attachments in updates/comments)
        """
        pdf_count = 0
        total_count = 0
        downloaded_count = 0
        
        # Query to get update assets (matching Java implementation)
        # We query for both 'url' and 'public_url':
        # - public_url: Publicly accessible but expires after ~1 hour
        # - url: Requires auth but doesn't expire
        # NOTE: We only query updates.assets (not items.assets) to match Java behavior
        # and avoid duplicate downloads of the same file
        query = f"""query {{
            items(ids: [{item_id}]) {{
                id
                updates(limit: 100) {{
                    id
                    body
                    assets {{
                        id
                        name
                        url
                        public_url
                        file_extension
                    }}
                }}
            }}
        }}"""
        
        payload = {"query": query}
        response = await self.http.post(payload)
        
        if "errors" in response:
            print(f"ERROR: GraphQL errors while fetching attachments: {response['errors']}")
            raise Exception(f"Failed to fetch attachments: {response['errors']}")
        
        items_node = response.get("data", {}).get("items", [])
        if not items_node:
            print(f"ERROR: No items returned for item ID: {item_id}")
            return False, 0
        
        item = items_node[0]
        
        # Collect assets from updates (matching Java implementation)
        all_assets = []
        
        updates = item.get("updates", [])
        if updates:
            for update in updates:
                update_assets = update.get("assets", [])
                for asset in update_assets:
                    all_assets.append(asset)
        
        if not all_assets:
            print(f"INFO: No attachments found for item {item_id}")
            return False, 0
        
        # Get email for filename
        email = await self.item_service.get_item_email(item_id, board_id)
        
        # Track seen asset IDs to avoid duplicates
        seen_asset_ids = set()
        
        for asset in all_assets:
            asset_id = asset.get("id", "")
            file_name = asset.get("name", "")
            public_url = asset.get("public_url", "")
            asset_url = asset.get("url", "")  # Authenticated URL (doesn't expire)
            file_extension = asset.get("file_extension", "")
            
            if not asset_id:
                print("WARNING: Skipping asset with no ID")
                continue
            
            # Skip duplicate assets
            if asset_id in seen_asset_ids:
                continue
            seen_asset_ids.add(asset_id)
            
            total_count += 1
            
            try:
                data = None
                last_error = None
                
                # Try download methods in order of preference:
                # 1. public_url (no auth needed - this is an S3/CDN URL)
                # 2. url with auth (requires Monday.com auth)
                
                # Try 1: public_url without auth (most common case)
                if not data and public_url and public_url != "null" and public_url.strip():
                    try:
                        if self.config.debug_mode:
                            print(f"   Trying public_url (no auth): {public_url[:100]}...")
                        data = await self.http.download_file(public_url, use_auth=False)
                        if self.config.debug_mode:
                            print(f"   Success! Downloaded {len(data)} bytes")
                    except Exception as e:
                        last_error = f"public_url (no auth): {e}"
                        if self.config.debug_mode:
                            print(f"   {last_error}")
                
                # Try 2: asset url with auth (authenticated Monday.com URL)
                if not data and asset_url and asset_url != "null" and asset_url.strip():
                    try:
                        if self.config.debug_mode:
                            print(f"   Trying url (with auth): {asset_url[:100]}...")
                        data = await self.http.download_file(asset_url, use_auth=True)
                        if self.config.debug_mode:
                            print(f"   Success! Downloaded {len(data)} bytes")
                    except Exception as e:
                        last_error = f"url (with auth): {e}"
                        if self.config.debug_mode:
                            print(f"   {last_error}")
                
                # If all methods failed, show the last error
                if data is None:
                    print(f"   Download FAILED for {file_name}: {last_error}")
                    continue
                
                # Ensure file has extension
                if '.' not in file_name and file_extension:
                    file_name = f"{file_name}.{file_extension}"
                
                # Check if file is PDF by extension OR by file content (PDF magic bytes)
                is_pdf = (
                    file_name.lower().endswith(".pdf") or 
                    file_extension.lower() == "pdf" or
                    data[:4] == b'%PDF'  # PDF magic bytes
                )
                
                if is_pdf:
                    pdf_count += 1
                
                # Format filename
                new_file_name = self._format_file_name(file_name, item_id, email, group_name)
                
                saved_path = await MondayFileDownloader.save(new_file_name, data, download_dir)
                print(f"Downloaded: {new_file_name}")
                downloaded_count += 1
                
            except Exception as e:
                print(f"ERROR: Failed to download asset {asset_id}: {e}")
        
        if total_count == 0:
            print(f"INFO: No attachments found for item {item_id}")
        else:
            print(f"Downloaded {downloaded_count}/{total_count} attachment(s) for item {item_id} ({pdf_count} PDF{'s' if pdf_count != 1 else ''})")
        
        return pdf_count > 0, pdf_count
    
    def _format_file_name(self, original_file_name: str, item_id: int, email: str, group_name: str) -> str:
        """Format filename: originalname__itemid__email__groupname.extension"""
        extension = ""
        name_without_ext = original_file_name
        
        last_dot = original_file_name.rfind('.')
        if last_dot > 0:
            extension = original_file_name[last_dot:]
            name_without_ext = original_file_name[:last_dot]
        
        # Sanitize all parts
        name_without_ext = self._sanitize_file_name(name_without_ext)
        email = self._sanitize_file_name(email)
        group_name = self._sanitize_file_name(group_name)
        
        return f"{name_without_ext}__{item_id}__{email}__{group_name}{extension}"
    
    def _sanitize_file_name(self, file_name: str) -> str:
        """Sanitize filename to remove invalid characters"""
        if not file_name:
            return "unknown"
        return re.sub(r'[<>:"|?*\\/]', '_', file_name).strip()


# ============================================================================
# Attachment Job (Main Orchestrator)
# ============================================================================

class MondayAttachmentJob:
    """Main job orchestrator for downloading attachments with parallel processing"""
    
    def __init__(self, config: MondayConfig):
        self.config = config
        self.http = MondayHttpClient(config)
        self.item_service = MondayItemService(config, self.http)
        self.attachment_service = MondayAttachmentService(config, self.http, self.item_service)
    
    async def run(self) -> dict:
        """Run the attachment download job"""
        print("Starting Monday.com attachment download job...")
        print(f"Workspace: {self.config.workspace_name}")
        print(f"Board: {self.config.board_name}")
        print("Filter Criteria:")
        print("   - Status = 'Retry': Process ALL items")
        print("   - Status = Empty: Process only today's and yesterday's date")
        print()
        
        total_success = 0
        total_failed = 0
        total_no_pdf = 0
        groups_processed = 0
        
        try:
            try:
                board_id = await self._resolve_board_id()
                print(f"Found board ID: {board_id}")
                print()
                
                await self.item_service.initialize_group_cache(board_id)
                print()
                
                await self._list_all_groups(board_id)
                print()
            except Exception as e:
                print(f"ERROR: Fatal error during initialization: {e}")
                return {"error": str(e), "capability": "download_attachments"}
            
            status_col_id = await self.item_service.get_status_column_id(board_id)
            
            # Process all groups in parallel
            group_tasks = []
            for group in self.config.groups:
                task = self._process_group(board_id, group, status_col_id)
                group_tasks.append(task)
            
            results = await asyncio.gather(*group_tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, Exception):
                    total_failed += 1
                elif result.get("processed"):
                    groups_processed += 1
                    total_success += result.get("success", 0)
                    total_failed += result.get("failed", 0)
                    total_no_pdf += result.get("no_pdf", 0)
            
            print("==========================================")
            print("Attachment download job completed")
            print(f"   Groups processed: {groups_processed} / {len(self.config.groups)}")
            print(f"   Success: {total_success}")
            print(f"   Failed: {total_failed}")
            print(f"   No PDF found: {total_no_pdf}")
            
            return {
                "result": {
                    "groups_processed": groups_processed,
                    "total_groups": len(self.config.groups),
                    "success": total_success,
                    "failed": total_failed,
                    "no_pdf": total_no_pdf
                },
                "capability": "download_attachments"
            }
        
        finally:
            # Always close the HTTP session to prevent resource leaks
            await self.http.close()
    
    async def _process_group(self, board_id: int, group_title: str, status_col_id: str) -> dict:
        """Process a single group with true streaming: fetch and download in parallel"""
        print("==========================================")
        print(f"Processing Group: {group_title}")
        
        download_folder = self.config.group_folder_map.get(group_title)
        if not download_folder:
            print(f"WARNING: No folder mapping found for group: {group_title}")
            return {"processed": False, "success": 0, "failed": 0, "no_pdf": 0}
        
        print(f"Download Folder: {download_folder}")
        
        success = 0
        failed = 0
        no_pdf = 0
        download_tasks = []  # Store background download tasks
        
        try:
            # TRUE STREAMING: Start downloads immediately as items are found
            # Downloads run in background while fetching continues
            def on_batch_found(batch_item_ids):
                print(f"Starting download for {len(batch_item_ids)} item(s)...")
                for item_id in batch_item_ids:
                    print(f"  Processing item ID: {item_id}")
                    # Create task immediately - runs in background while fetching continues
                    task = asyncio.create_task(
                        self._process_item(board_id, item_id, download_folder, group_title)
                    )
                    download_tasks.append(task)
            
            # This streams items AND starts downloads as they're found
            await self.item_service.stream_item_ids_from_group(
                board_id,
                group_title,
                2**31 - 1,  # No limit - process all matching items
                status_col_id,
                self.config.target_status,
                on_batch_found
            )
            
            # Wait for all download tasks to complete
            if download_tasks:
                item_results = await asyncio.gather(*download_tasks, return_exceptions=True)
                
                for result in item_results:
                    if isinstance(result, Exception):
                        failed += 1
                    elif result == "success":
                        success += 1
                    elif result == "no_pdf":
                        no_pdf += 1
                    else:
                        failed += 1
            else:
                print(f"INFO: No items found in group: {group_title}")
        
        except Exception as e:
            print(f"ERROR: Failed to process group {group_title}: {e}")
            return {"processed": True, "success": 0, "failed": 1, "no_pdf": 0}
        
        print(f"Group summary: {success} succeeded, {failed} failed, {no_pdf} with no PDF")
        print()
        
        return {"processed": True, "success": success, "failed": failed, "no_pdf": no_pdf}
    
    async def _process_item(self, board_id: int, item_id: int, download_folder: str, group_name: str) -> str:
        """Process a single item (download attachments and update status)"""
        try:
            has_pdf, pdf_count = await self.attachment_service.download_attachments(
                item_id, download_folder, board_id, group_name
            )
            
            if not has_pdf:
                print(f"  WARNING: No PDF found for item {item_id} - updating status to '{self.config.error_status}'")
                await self.item_service.update_status(item_id, self.config.error_status, board_id)
                return "no_pdf"
            
            await self.item_service.update_status(item_id, self.config.new_status, board_id)
            print(f"  Successfully processed item {item_id} ({pdf_count} PDF{'s' if pdf_count != 1 else ''})")
            
            return "success"
        
        except Exception as e:
            print(f"  ERROR: Failed to process item {item_id}: {e}")
            return "failed"
    
    async def _resolve_board_id(self) -> int:
        """Resolve board ID by board name"""
        query = """query {
            workspaces(limit: 100) {
                id name
            }
        }"""
        
        payload = {"query": query}
        response = await self.http.post(payload)
        
        if "errors" in response:
            print(f"WARNING: Workspace query had errors (this is okay, using fallback): {response['errors']}")
            return await self._resolve_board_id_simple()
        
        workspaces = response.get("data", {}).get("workspaces", [])
        
        for workspace in workspaces:
            workspace_name = workspace.get("name", "")
            if self.config.workspace_name == workspace_name:
                workspace_id = workspace.get("id")
                print(f"Found workspace '{workspace_name}' (ID: {workspace_id})")
                break
        
        return await self._resolve_board_id_simple()
    
    async def _resolve_board_id_simple(self) -> int:
        """Simple board resolution (fallback)"""
        query = "query { boards(limit: 500) { id name } }"
        
        payload = {"query": query}
        response = await self.http.post(payload)
        
        if "errors" in response:
            raise Exception(f"Failed to query boards: {response['errors']}")
        
        boards = response.get("data", {}).get("boards", [])
        
        for board in boards:
            board_name = board.get("name", "")
            if self.config.board_name == board_name:
                return int(board.get("id"))
        
        raise Exception(f"Board not found: {self.config.board_name}")
    
    async def _list_all_groups(self, board_id: int) -> None:
        """List all groups in the board for debugging"""
        query = f"""query {{
            boards(ids: {board_id}) {{
                groups {{ id title }}
            }}
        }}"""
        
        payload = {"query": query}
        response = await self.http.post(payload)
        
        if "errors" in response:
            print(f"WARNING: Could not list groups: {response['errors']}")
            return
        
        boards = response.get("data", {}).get("boards", [])
        if not boards:
            return
        
        groups = boards[0].get("groups", [])
        if groups:
            print("Groups found in board:")
            for group in groups:
                group_title = group.get("title", "")
                print(f"   - {group_title}")


# ============================================================================
# Capability: List Groups
# ============================================================================

async def list_groups(args: dict) -> dict:
    """Lists all groups in a Monday.com board"""
    config = MondayConfig(args)
    http = MondayHttpClient(config)
    
    try:
        # Resolve board ID
        query = "query { boards(limit: 500) { id name } }"
        response = await http.post({"query": query})
        
        if "errors" in response:
            return {"error": str(response["errors"]), "capability": "list_groups"}
        
        boards = response.get("data", {}).get("boards", [])
        board_id = None
        
        for board in boards:
            if board.get("name") == config.board_name:
                board_id = int(board.get("id"))
                break
        
        if not board_id:
            return {"error": f"Board not found: {config.board_name}", "capability": "list_groups"}
        
        # Get groups
        query = f"""query {{
            boards(ids: {board_id}) {{
                groups {{ id title }}
            }}
        }}"""
        
        response = await http.post({"query": query})
        
        if "errors" in response:
            return {"error": str(response["errors"]), "capability": "list_groups"}
        
        boards = response.get("data", {}).get("boards", [])
        if not boards:
            return {"error": "Board not found", "capability": "list_groups"}
        
        groups = boards[0].get("groups", [])
        group_list = [{"id": g.get("id"), "title": g.get("title")} for g in groups]
        
        return {
            "result": {
                "board_id": board_id,
                "board_name": config.board_name,
                "groups": group_list
            },
            "capability": "list_groups"
        }
    
    finally:
        await http.close()


# ============================================================================
# Capability: Get Item Status
# ============================================================================

async def get_item_status(args: dict) -> dict:
    """Gets the current status of a specific item"""
    config = MondayConfig(args)
    http = MondayHttpClient(config)
    item_service = MondayItemService(config, http)
    
    try:
        item_id = args.get("item_id")
        if not item_id:
            return {"error": "item_id is required", "capability": "get_item_status"}
        
        # Resolve board ID
        query = "query { boards(limit: 500) { id name } }"
        response = await http.post({"query": query})
        
        if "errors" in response:
            return {"error": str(response["errors"]), "capability": "get_item_status"}
        
        boards = response.get("data", {}).get("boards", [])
        board_id = None
        
        for board in boards:
            if board.get("name") == config.board_name:
                board_id = int(board.get("id"))
                break
        
        if not board_id:
            return {"error": f"Board not found: {config.board_name}", "capability": "get_item_status"}
        
        # Get status column ID
        status_col_id = await item_service.get_status_column_id(board_id)
        
        # Get item status
        query = f"""query {{
            items(ids: [{item_id}]) {{
                id
                name
                column_values {{ id text }}
            }}
        }}"""
        
        response = await http.post({"query": query})
        
        if "errors" in response:
            return {"error": str(response["errors"]), "capability": "get_item_status"}
        
        items = response.get("data", {}).get("items", [])
        if not items:
            return {"error": f"Item not found: {item_id}", "capability": "get_item_status"}
        
        item = items[0]
        columns = item.get("column_values", [])
        
        status = ""
        for col in columns:
            if col.get("id") == status_col_id:
                status = col.get("text", "")
                break
        
        return {
            "result": {
                "item_id": item_id,
                "item_name": item.get("name"),
                "status": status
            },
            "capability": "get_item_status"
        }
    
    finally:
        await http.close()


# ============================================================================
# Capability: Update Item Status
# ============================================================================

async def update_item_status(args: dict) -> dict:
    """Updates the status of a specific item"""
    config = MondayConfig(args)
    http = MondayHttpClient(config)
    item_service = MondayItemService(config, http)
    
    try:
        item_id = args.get("item_id")
        new_status = args.get("new_status")
        
        if not item_id:
            return {"error": "item_id is required", "capability": "update_item_status"}
        if not new_status:
            return {"error": "new_status is required", "capability": "update_item_status"}
        
        # Resolve board ID
        query = "query { boards(limit: 500) { id name } }"
        response = await http.post({"query": query})
        
        if "errors" in response:
            return {"error": str(response["errors"]), "capability": "update_item_status"}
        
        boards = response.get("data", {}).get("boards", [])
        board_id = None
        
        for board in boards:
            if board.get("name") == config.board_name:
                board_id = int(board.get("id"))
                break
        
        if not board_id:
            return {"error": f"Board not found: {config.board_name}", "capability": "update_item_status"}
        
        # Update status
        await item_service.update_status(item_id, new_status, board_id)
        
        return {
            "result": {
                "item_id": item_id,
                "new_status": new_status,
                "success": True
            },
            "capability": "update_item_status"
        }
    
    finally:
        await http.close()


# ============================================================================
# Capability: Download Attachments
# ============================================================================

async def download_attachments(args: dict) -> dict:
    """Downloads all attachments from specified Monday.com board groups"""
    try:
        config = MondayConfig(args)
        
        # Validate required parameters
        if not config.api_token:
            return {"error": "Missing required parameter: api_token", "capability": "download_attachments"}
        if not config.board_name:
            return {"error": "Missing required parameter: board_name", "capability": "download_attachments"}
        if not config.groups:
            return {"error": "Missing or empty required parameter: groups", "capability": "download_attachments"}
        if not config.group_folder_map:
            return {"error": "Missing or empty required parameter: group_folder_map", "capability": "download_attachments"}
        
        job = MondayAttachmentJob(config)
        return await job.run()
    
    except Exception as e:
        return {"error": f"Unexpected error: {str(e)}", "capability": "download_attachments"}


# ============================================================================
# Main Entry Point
# ============================================================================

def main():
    """Main entry point - reads JSON from stdin, outputs JSON to stdout"""
    try:
        input_data = json.load(sys.stdin)
        
        capability = input_data.get("capability")
        args = input_data.get("args", {})
        
        if capability == "download_attachments":
            result = asyncio.run(download_attachments(args))
            print(json.dumps(result, indent=2))
        
        elif capability == "list_groups":
            result = asyncio.run(list_groups(args))
            print(json.dumps(result, indent=2))
        
        elif capability == "get_item_status":
            result = asyncio.run(get_item_status(args))
            print(json.dumps(result, indent=2))
        
        elif capability == "update_item_status":
            result = asyncio.run(update_item_status(args))
            print(json.dumps(result, indent=2))
        
        else:
            print(json.dumps({
                "error": f"Unknown capability: {capability}",
                "capability": capability
            }, indent=2))
    
    except Exception as e:
        print(json.dumps({
            "error": f"Error: {str(e)}",
            "capability": "unknown"
        }, indent=2))
        sys.exit(1)


if __name__ == "__main__":
    main()
