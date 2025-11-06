import requests
import json
import re
from typing import Dict, Any


class AnnualReportExtractor:
    def __init__(self, api_key: str, base_url: str, model_id: str):
        self.api_key = api_key
        self.base_url = base_url
        self.model_id = model_id
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }

    def extract_structured_data(self, text: str) -> Dict[str, Any]:
        """Extract structured data using the GPT-OSS model"""

        extraction_prompt = f"""
You are a financial analyst extracting key information from annual reports.

Extract the following information from this annual report text and return it as valid JSON:

{{
    "company_name": "string",
    "report_year": "integer",
    "financial_metrics": {{
        "total_revenue": "number or null",
        "net_profit": "number or null",
        "total_assets": "number or null",
        "shareholders_equity": "number or null",
        "revenue_growth_percent": "number or null"
    }},
    "operational_metrics": {{
        "number_of_employees": "number or null",
        "number_of_customers_or_passengers": "number or null",
        "key_operational_figure": "string or null"
    }},
    "esg_metrics": {{
        "carbon_emissions": "number or null",
        "employee_training_hours": "number or null",
        "esg_rating": "string or null"
    }}
}}

If a value cannot be found, use null. Extract numbers without currency symbols or units when possible.

Annual Report Text:
{text[:30000]}

Return only valid JSON:
"""

        payload = {
            "model": self.model_id,
            "messages": [
                {"role": "system", "content": "You are a precise financial data extraction assistant. Always return valid JSON."},
                {"role": "user", "content": extraction_prompt}
            ],
            "max_tokens": 2048,
            "temperature": 0.1  # Low temperature for consistency
        }

        try:
            response = requests.post(
                f"{self.base_url}/chat/completions",
                headers=self.headers,
                json=payload,
                timeout=120
            )
            response.raise_for_status()

            result = response.json()
            assistant_response = result['choices'][0]['message']['content']

            # Try to parse JSON from response
            json_match = re.search(r'\{.*\}', assistant_response, re.DOTALL)
            if json_match:
                return json.loads(json_match.group())
            else:
                return {"error": "No valid JSON found in response"}

        except Exception as e:
            return {"error": f"Extraction failed: {str(e)}"}


class EnhancedAnnualReportExtractor:
    def __init__(self, api_key: str, base_url: str, model_id: str):
        self.api_key = api_key
        self.base_url = base_url
        self.model_id = model_id
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }

    def create_extraction_prompt(self, text: str, file_name: str = "") -> str:
        """Create a more detailed extraction prompt"""
        industry = self._detect_industry(text, file_name)
        operational_fields = self._build_operational_schema(industry)

        operational_schema_lines = []
        for field_name, description in operational_fields:
            operational_schema_lines.append(f'        "{field_name}": "{description}"')
        operational_schema = ",\n".join(operational_schema_lines)

        industry_instruction = (
            "Tailor operational metrics to the banking sector (focus on customers, accounts, branches, etc.)."
            if industry == "banking"
            else "Tailor operational metrics to the airline/aviation sector (focus on passengers, routes, fleet, etc.)."
            if industry == "airline"
            else "Keep operational metrics generic and only include figures explicitly mentioned in the report."
        )

        return f"""
You are analyzing an annual report. Extract key financial and operational data.

FILE: {file_name}

INSTRUCTIONS:
- Look for the most recent fiscal year data
- Convert all monetary values to millions (if in thousands or billions, convert to millions)
- Return only valid JSON
- Use null for missing values
- Be precise with numbers
- {industry_instruction}

REQUIRED JSON FORMAT:
{{
    "company_name": "string",
    "report_year": "integer (fiscal year)",
    "currency": "string (EUR, USD, GBP, etc.)",
    "financial_metrics": {{
        "total_revenue": "number in millions",
        "net_profit": "number in millions",
        "total_assets": "number in millions",
        "shareholders_equity": "number in millions",
        "operating_costs": "number in millions",
        "total_debt": "number in millions"
    }},
    "operational_metrics": {{
{operational_schema}
    }},
    "sustainability": {{
        "co2_emissions": "number if mentioned",
        "sustainability_rating": "string if mentioned",
        "employee_training_hours": "number if mentioned"
    }},
    "extraction_confidence": "integer 1-10"
}}

ANNUAL REPORT TEXT:
{text[:35000]}

Return only the JSON object:
"""

    def _detect_industry(self, text: str, file_name: str = "") -> str:
        """Rudimentary industry detection based on filename and text."""
        sample = f"{file_name} {text[:2000]}".lower()

        banking_keywords = [
            "bank",
            "banking",
            "financial services",
            "credit union",
            "loan portfolio",
            "deposits",
            "interest income",
        ]

        airline_keywords = [
            "airline",
            "airways",
            "aviation",
            "passenger",
            "fleet",
            "route network",
        ]

        if any(keyword in sample for keyword in banking_keywords):
            return "banking"
        if any(keyword in sample for keyword in airline_keywords):
            return "airline"
        return "general"

    def _build_operational_schema(self, industry: str):
        """Return operational metrics schema entries based on the detected industry."""
        if industry == "banking":
            return [
                ("employees", "integer"),
                ("customer_accounts", "integer (total active customer accounts)"),
                ("branches", "integer (number of branches or offices)"),
                ("key_operational_metric", "string description of any other disclosed operational metric"),
            ]

        if industry == "airline":
            return [
                ("employees", "integer"),
                ("passengers_carried", "integer (total annual passengers)"),
                ("routes_served", "integer (destinations or routes)"),
                ("key_operational_metric", "string description of any other disclosed operational metric"),
            ]

        return [
            ("employees", "integer"),
            ("customers", "integer if a customer count is explicitly stated"),
            ("locations", "integer (number of sites, stores, or facilities if mentioned)"),
            ("key_operational_metric", "string description of any other disclosed operational metric"),
        ]

    def extract_structured_data(self, text: str, file_name: str = "") -> Dict[str, Any]:
        """Extract with improved error handling and validation"""

        extraction_prompt = self.create_extraction_prompt(text, file_name)

        payload = {
            "model": self.model_id,
            "messages": [
                {"role": "system", "content": "You are a precise financial analyst. Always return valid JSON. Be accurate with numbers and conservative with confidence scores."},
                {"role": "user", "content": extraction_prompt}
            ],
            "max_tokens": 2048,
            "temperature": 0.1
        }

        try:
            response = requests.post(
                f"{self.base_url}/chat/completions",
                headers=self.headers,
                json=payload,
                timeout=120
            )
            response.raise_for_status()

            result = response.json()
            assistant_response = result['choices'][0]['message']['content']

            # Extract JSON more robustly
            cleaned_response = self._extract_json(assistant_response)
            return cleaned_response

        except Exception as e:
            return {
                "error": f"Extraction failed: {str(e)}",
                "raw_response": assistant_response if 'assistant_response' in locals() else None
            }

    def _extract_json(self, text: str) -> Dict:
        """More robust JSON extraction"""
        # Try to find JSON block
        json_patterns = [
            r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}',  # Nested braces
            r'\{.*\}',  # Simple braces
        ]

        for pattern in json_patterns:
            matches = re.finditer(pattern, text, re.DOTALL)
            for match in matches:
                try:
                    return json.loads(match.group())
                except json.JSONDecodeError:
                    continue

        # If no valid JSON found, return error structure
        return {
            "error": "No valid JSON found in response",
            "raw_text": text[:500]
        }