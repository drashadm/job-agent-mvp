from __future__ import annotations

from typing import Any, Dict, Optional, List
import requests
from urllib.parse import quote


class AirtableClient:
    """
    Minimal Airtable REST client for MVP.
    - create_record
    - update_record
    - find_one (by filterByFormula)
    - get_record (optional convenience)
    """

    def __init__(self, token: str, base_id: str, timeout_s: int = 30):
        self.base_id = base_id
        self.base_url = f"https://api.airtable.com/v0/{base_id}"
        self.timeout_s = timeout_s
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
        )

    def _table_url(self, table: str) -> str:
        # Table names can include spaces; quote safely
        return f"{self.base_url}/{quote(table, safe='')}"

    def create_record(self, table: str, fields: Dict[str, Any]) -> Dict[str, Any]:
        url = self._table_url(table)
        payload = {"fields": fields}
        resp = self.session.post(url, json=payload, timeout=self.timeout_s)
        self._raise_airtable(resp)
        return resp.json()

    def update_record(self, table: str, record_id: str, fields: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self._table_url(table)}/{record_id}"
        payload = {"fields": fields}
        resp = self.session.patch(url, json=payload, timeout=self.timeout_s)
        self._raise_airtable(resp)
        return resp.json()

    def get_record(self, table: str, record_id: str) -> Dict[str, Any]:
        url = f"{self._table_url(table)}/{record_id}"
        resp = self.session.get(url, timeout=self.timeout_s)
        self._raise_airtable(resp)
        return resp.json()

    def find_one(self, table: str, id_field: str, id_value: str) -> Optional[Dict[str, Any]]:
        """
        Find first record where {id_field} == id_value
        """
        url = self._table_url(table)
        # Airtable formulas prefer double-quotes for strings
        params = {"filterByFormula": f'{{{id_field}}}="{id_value}"', "maxRecords": 1}
        resp = self.session.get(url, params=params, timeout=self.timeout_s)
        self._raise_airtable(resp)
        records = resp.json().get("records", [])
        return records[0] if records else None

    def list_records(self, table: str, *, max_records: int = 100, filter_by_formula: Optional[str] = None) -> List[Dict[str, Any]]:
        url = self._table_url(table)
        params: Dict[str, Any] = {"maxRecords": max_records}
        if filter_by_formula is not None:
            params["filterByFormula"] = filter_by_formula

        resp = self.session.get(url, params=params, timeout=self.timeout_s)
        self._raise_airtable(resp)
        return resp.json().get("records", [])

    def get_table_schema(self, table_name: str) -> Optional[set]:
        """
        Fetch table schema via Airtable Meta API.
        Returns a set of field names (strings) for the given table.
        Returns None if schema cannot be fetched.
        """
        try:
            meta_url = f"https://api.airtable.com/v0/meta/bases/{self.base_id}/tables"
            resp = self.session.get(meta_url, timeout=self.timeout_s)
            self._raise_airtable(resp)
            
            data = resp.json()
            tables = data.get("tables", [])
            
            for table in tables:
                if table.get("name") == table_name:
                    fields = table.get("fields", [])
                    field_names = {f.get("name") for f in fields if f.get("name")}
                    return field_names
            
            return None
        except Exception:
            return None

    @staticmethod
    def _raise_airtable(resp: requests.Response) -> None:
        if resp.status_code < 400:
            return
        # Airtable returns helpful JSON error payloads
        try:
            err = resp.json()
        except Exception:
            err = {"message": resp.text}
        raise RuntimeError(f"Airtable API error {resp.status_code}: {err}")
