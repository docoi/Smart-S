import os
import re
import json
from typing import Dict, Any
from openai import OpenAI

DEFAULT_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o")  # fast + smart by default


class ExpertEmailGenerator:
    def __init__(self):
        self.pfp_offer_summary = (
            "We help businesses ensure full fire safety compliance and protect their people "
            "through expert fire risk assessments, training, and ongoing support—saving them "
            "from costly fines and ensuring peace of mind."
        )
        self.your_name = "Dene Plumbridge"
        self.your_title = "PFP Fire Protection Professional"
        self.your_email_signature = (
            "Fire Safety Services\n\n"
            "📞 01908 103 303\n"
            "🌐 https://p-f-p.co.uk/"
        )

        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY not set")
        self.client = OpenAI(api_key=api_key)

    # ---------- PUBLIC ----------

    def generate_expert_cold_email(
        self, contact: Dict[str, Any], company_data: Dict[str, Any], pfp_context: Dict[str, Any]
    ) -> Dict[str, Any]:
        try:
            payload = self._prepare_email_data(contact, company_data)
            prompt = self._build_prompt(payload)
            result_text = self._call_openai(prompt)
            return self._parse_response(result_text, contact, company_data)
        except Exception as e:
            print(f"Expert email generation error: {e}")
            return self._fallback_email(contact, company_data)

    def generate_multiple_subject_lines(
        self, contact: Dict[str, Any], company_data: Dict[str, Any], count: int = 3
    ):
        payload = self._prepare_email_data(contact, company_data)
        prompt = (
            f"Generate {count} short (3–7 words) cold-email subject lines for "
            f"{payload['target_name']} ({payload['job_title']}) at {payload['company_name']}. "
            f"Tone: value or curiosity, no spammy words. Company context: {payload['website_summary']}\n\n"
            f"Return one subject per line. No prefixes."
        )
        try:
            resp = self.client.chat.completions.create(
                model=DEFAULT_MODEL,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=180,
                temperature=0.8,
            )
            lines = [
                ln.strip() for ln in resp.choices[0].message.content.strip().split("\n") if ln.strip()
            ]
            return lines[:count] or [f"Quick fire safety check – {payload['company_name']}"]
        except Exception as e:
            print(f"Subject gen error: {e}")
            return [f"Quick fire safety check – {payload['company_name']}"]

    # ---------- INTERNALS ----------

    def _prepare_email_data(self, contact, company):
        name = contact.get("name") or "there"
        first = name.split()[0] if name != "there" else "there"
        website_summary = self._summarise_company(company)
        hooks = self._personal_hooks(contact, company)
        title = contact.get("title") or contact.get("role") or "Decision Maker"
        linkedin_bio = f"{title} at {company.get('company_name','Company')}"
        if contact.get("linkedin_profile_url"):
            linkedin_bio += f" - {contact['linkedin_profile_url']}"

        return {
            "target_name": name,
            "target_first_name": first,
            "job_title": title,
            "company_name": company.get("company_name", "Your Company"),
            "company_url": company.get("url", ""),
            "website_summary": website_summary,
            "linkedin_bio": linkedin_bio,
            "personal_hooks": hooks,
            "your_offer_summary": self.pfp_offer_summary,
        }

    def _summarise_company(self, company):
        parts = []
        if company.get("company_name"):
            parts.append(f"Company: {company['company_name']}")
        if company.get("industry"):
            parts.append(f"Industry: {company['industry']}")
        if company.get("location"):
            parts.append(f"Location: {company['location']}")
        services = company.get("services") or []
        if services:
            parts.append(f"Services: {', '.join(services[:3])}")
        fire_kw = company.get("fire_safety_keywords") or []
        if fire_kw:
            parts.append(f"Fire safety mentions: {', '.join(fire_kw[:2])}")
        compliance = company.get("compliance_mentions") or []
        if compliance:
            parts.append(f"Compliance awareness: {', '.join(compliance[:2])}")
        about = (company.get("about_text") or "").strip()
        if about:
            parts.append(f"About: {about[:100]}...")
        return "; ".join(parts)

    def _personal_hooks(self, contact, company):
        hooks = list(company.get("personalization_hooks") or [])
        role = (contact.get("title") or contact.get("role") or "").lower()
        if "facilities" in role:
            hooks.append("Facilities role suggests responsibility for building fire safety")
        elif "operations" in role:
            hooks.append("Operations role oversees premises safety procedures")
        elif "safety" in role:
            hooks.append("Safety role indicates direct fire protection responsibility")
        elif "owner" in role or "director" in role:
            hooks.append("Leadership with ultimate workplace fire compliance responsibility")
        elif "manager" in role:
            hooks.append("Management responsibility for building safety standards")

        industry = (company.get("industry") or "").lower()
        if "manufactur" in industry:
            hooks.append("Manufacturing premises need comprehensive protection")
        elif "health" in industry:
            hooks.append("Healthcare facilities require specialised compliance")
        elif "educat" in industry:
            hooks.append("Education sites have complex obligations")
        elif "event" in industry:
            hooks.append("Event offices/warehouses must maintain compliance")
        elif "retail" in industry:
            hooks.append("Retail premises require FRAs and systems")
        else:
            hooks.append("Business premises require fire safety compliance and protection")

        loc = company.get("location")
        if loc and loc != "UK":
            hooks.append(f"Based in {loc}")
        return "; ".join(hooks[:3])

    def _build_prompt(self, d):
        return f"""
You are a world-class B2B cold email expert trained on millions of high-converting campaigns. Write a
first-touch email with a killer subject line using the inputs below.

OBJECTIVE:
A one-to-one, personalised first cold email. Confident, helpful, conversational. No spammy vibe.

INPUTS:
1) Target Name: {d['target_name']}
2) Target Job Title: {d['job_title']}
3) Company Name: {d['company_name']}
4) Company Website URL: {d['company_url']}
5) Scraped Website Summary: {d['website_summary']}
6) LinkedIn Bio / Notes: {d['linkedin_bio']}
7) Personal Hooks: {d['personal_hooks']}
8) Offer Summary: {d['your_offer_summary']}

OUTPUT FORMAT (use exactly):
Subject Line: <3–7 words, curiosity/value-driven>

Body:
Hi {d['target_first_name']},

[custom hook proving this is not mass-mail — from company or personal info.]

[one-sentence micro case study/outcome.]

[one-sentence soft CTA inviting reply or a short call.]

Regards,

{self.your_name}
{self.your_title}

{self.your_email_signature}

RULES:
- No buzzwords (synergy/disrupt/etc). No hard sell.
- Make it feel uniquely written for them.
- Focus on BUILDING/PREMISES fire safety compliance (FRA, emergency lighting, alarms).
- Do NOT pitch their services; focus on their own workplace compliance.
- Proper paragraph spacing (blank line between thoughts).
- Keep sentences tight and readable.
"""

    def _call_openai(self, prompt: str) -> str:
        resp = self.client.chat.completions.create(
            model=DEFAULT_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=700,
            temperature=0.65,
        )
        return resp.choices[0].message.content

    def _parse_response(self, txt: str, contact, company):
        lines = (txt or "").splitlines()
        subject = ""
        body_lines = []
        in_body = False
        
        for ln in lines:
            # Extract subject line
            if ln.strip().lower().startswith("subject line:"):
                subject = ln.split(":", 1)[-1].strip().strip("'\"")
                continue
            
            # Start capturing body after "Body:" marker
            if ln.strip().lower().startswith("body:"):
                in_body = True
                continue
            
            # Skip the subject line if it appears again in the body
            if ln.strip().lower().startswith("subject line:") and in_body:
                continue
                
            # Capture body content
            if in_body:
                body_lines.append(ln)

        body = "\n".join(body_lines).strip() or txt.strip()
        
        # Clean up the body to remove any remaining subject line references
        body = self._format_body(body)
        
        # Remove any lingering "Subject Line:" text from the body
        body = re.sub(r'^Subject Line:.*?\n\n?', '', body, flags=re.MULTILINE | re.IGNORECASE)
        body = re.sub(r'Subject Line:.*?\n', '', body, flags=re.IGNORECASE)

        if not subject:
            subject = f"Quick fire safety check – {company.get('company_name', 'your business')}"
            
        return {
            "subject": subject,
            "body": body,
            "personalization_notes": f"Expert framework applied for {contact.get('name','contact')}",
            "compliance_angle": "Workplace compliance",
            "call_to_action": "Soft reply",
            "expert_framework": True,
        }

    def _format_body(self, content: str) -> str:
        content = content.strip()
        
        # Clean any artifacts including subject line references
        content = re.sub(r'(INTENDED FOR|FIRE PROTECTION SCORE|REASON|--- EMAIL CONTENT ---).*?\n', '', content, flags=re.I)
        content = re.sub(r'^Subject Line:.*?\n\n?', '', content, flags=re.MULTILINE | re.IGNORECASE)
        content = re.sub(r'Subject Line:.*?\n', '', content, flags=re.IGNORECASE)
        
        # Replace em dashes with commas for better professional appearance
        content = re.sub(r'—', ',', content)
        content = re.sub(r'–', ',', content)  # Also handle en dashes
        
        # Spacing after greeting
        content = re.sub(r'(Hi [^,]+,)\s*', r'\1\n\n', content)
        
        # Ensure blank lines between sentences occasionally
        content = re.sub(r'([.!?])\s+([A-Z])', r'\1\n\n\2', content)
        
        # Spacing before closing
        content = re.sub(r'([.!?])\s+(Best,|Regards,|Kind regards,)', r'\1\n\n\2', content)
        
        # Tighten multiple newlines
        content = re.sub(r'\n{3,}', '\n\n', content)
        
        return content

    def _fallback_email(self, contact, company):
        name = contact.get("name") or "there"
        first = name.split()[0] if name != "there" else "there"
        company_name = company.get("company_name", "your business")
        subject = "Quick fire safety question"
        body = (
            f"Hi {first},\n\n"
            f"I noticed {company_name} — your premises will need comprehensive fire safety compliance.\n\n"
            "We help similar businesses avoid penalties and keep people safe via expert fire risk assessments "
            "and ongoing support.\n\n"
            "Worth a brief chat to see if we can help?\n\n"
            f"Regards,\n\n{self.your_name}\n{self.your_title}\n\n{self.your_email_signature}"
        )
        return {
            "subject": subject,
            "body": body,
            "personalization_notes": "Fallback template used",
            "compliance_angle": "General fire safety",
            "call_to_action": "Brief chat invitation",
            "expert_framework": False,
        }


if __name__ == "__main__":
    gen = ExpertEmailGenerator()
    demo_contact = {"name": "John Smith", "title": "Facilities Manager", "email": "john@techcorp.com", "source": "linkedin"}
    demo_company = {
        "company_name": "TechCorp Manufacturing Ltd",
        "industry": "manufacturing",
        "location": "Birmingham, UK",
        "services": ["CNC machining", "Metal fabrication", "Assembly"],
        "fire_safety_keywords": ["health and safety", "fire risk"],
        "compliance_mentions": ["building regulations"],
        "personalization_hooks": ["recent expansion", "new facility"],
        "about_text": "Leading manufacturer of precision components for automotive and aerospace industries.",
        "url": "https://techcorp.com",
    }
    out = gen.generate_expert_cold_email(demo_contact, demo_company, {})
    print("Subject:", out["subject"])
    print("\n", out["body"])
