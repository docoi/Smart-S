"""
🌐 Smart Website Scraper Module
==============================
Captures ALL useful data in Part 1 - no re-scraping needed!
"""

import json
import re
from urllib.parse import urlparse, urlunparse
from account_manager import get_working_apify_client_part1


class WebsiteScraper:
    """🌐 Smart website scraping - capture everything once in Part 1"""
    
    def __init__(self, openai_key):
        self.openai_key = openai_key
        # Store fallback data from Part 1
        self.fallback_contact_data = {
            'emails_found': [],
            'contact_info': [],
            'all_content': '',
            'domain': ''
        }
    
    def scrape_website_for_staff_and_linkedin(self, website_url: str) -> tuple:
        """📋 SMART: Scrape website once and capture ALL useful data for later use"""
        
        print(f"🗺️ Phase 1a: Website mapping...")
        
        # Normalize URL and extract domain
        normalized_url = self._normalize_www(website_url)
        domain = urlparse(website_url).netloc.replace('www.', '')
        self.fallback_contact_data['domain'] = domain
        
        # Map website to find URLs using account rotation
        website_map = self._stealth_website_mapping(normalized_url)
        
        if not website_map:
            print("❌ Website mapping failed")
            return [], ""
        
        print(f"🧠 Phase 1b: GPT-4o URL analysis...")
        
        # Analyze with GPT-4o to select best URLs
        selected_urls = self._gpt_analyze_urls(website_map, normalized_url)
        
        if not selected_urls:
            print("❌ No URLs selected for analysis")
            return [], ""
        
        print(f"🔍 Phase 1c: SMART content analysis - capturing ALL data...")
        
        # 🧠 SMART: Extract staff, LinkedIn, AND capture fallback data
        staff_list, social_data = self._extract_staff_and_capture_fallback_data(selected_urls, domain)
        
        # Find LinkedIn URL
        linkedin_url = (social_data.get('company_linkedin') or 
                       social_data.get('linkedin') or 
                       social_data.get('LinkedIn') or "")
        
        print(f"✅ Website scraping complete:")
        print(f"   👥 Staff found: {len(staff_list)}")
        print(f"   🔗 LinkedIn URL: {linkedin_url[:50]}..." if linkedin_url else "   ❌ No LinkedIn URL found")
        print(f"   📧 Fallback emails captured: {len(self.fallback_contact_data['emails_found'])}")
        print(f"   👤 Fallback contacts captured: {len(self.fallback_contact_data['contact_info'])}")
        print(f"   💾 Fallback data ready for later use if needed")
        
        return staff_list, linkedin_url

    def get_fallback_contacts(self) -> list:
        """🛡️ Return pre-captured fallback contact data (no re-scraping needed!)"""
        
        print("\n🛡️ USING PRE-CAPTURED FALLBACK DATA")
        print("🎯 Strategy: Use contact data already captured in Part 1")
        print("✅ No re-scraping needed - data already available!")
        print("=" * 70)
        
        domain = self.fallback_contact_data['domain']
        emails_found = self.fallback_contact_data['emails_found']
        contact_info = self.fallback_contact_data['contact_info']
        
        if not emails_found and not contact_info:
            print("❌ No fallback contact data available")
            return []
        
        print(f"📧 Using {len(emails_found)} pre-captured emails")
        print(f"👤 Using {len(contact_info)} pre-captured contacts")
        
        # Generate emails for discovered contacts using pre-captured data
        discovered_contacts = self._generate_emails_for_captured_contacts(
            contact_info, emails_found, domain
        )
        
        # Score contacts for fire protection relevance
        scored_contacts = self._score_website_contacts_fire_protection(discovered_contacts)
        
        print(f"\n📊 FALLBACK RESULTS FROM PRE-CAPTURED DATA:")
        print(f"   📧 Direct emails: {len(emails_found)}")
        print(f"   👥 Named contacts: {len(contact_info)}")
        print(f"   ✅ Total verified contacts: {len(scored_contacts)}")
        
        return scored_contacts

    def _extract_staff_and_capture_fallback_data(self, urls: list, domain: str) -> tuple:
        """🧠 SMART: Extract staff AND capture all contact data for later fallback use"""
        
        all_staff = []
        all_social = {}
        all_content_combined = ""
        
        for i, url in enumerate(urls, 1):
            print(f"  📄 Processing {i}/{len(urls)}: {url}")
            
            try:
                staff, social, content = self._analyze_single_url_with_content_capture(url)
                all_staff.extend(staff)
                all_social.update(social)
                
                # 🧠 SMART: Accumulate ALL content for fallback analysis
                if content:
                    all_content_combined += f"\n\n=== CONTENT FROM {url} ===\n{content}"
                
                if staff:
                    print(f"    ✅ Found {len(staff)} staff members")
                
            except Exception as e:
                print(f"    ⚠️ Error: {e}")
        
        # 🧠 SMART: Now analyze ALL captured content for fallback contacts
        print(f"\n🧠 SMART ANALYSIS: Extracting fallback data from {len(all_content_combined)} characters")
        self.fallback_contact_data['all_content'] = all_content_combined
        
        # Extract emails from all content
        emails_found = self._extract_emails_from_content(all_content_combined)
        self.fallback_contact_data['emails_found'] = emails_found
        
        # Extract contact information using GPT-4o-mini
        contact_info = self._extract_contact_info_gpt(all_content_combined, domain)
        self.fallback_contact_data['contact_info'] = contact_info
        
        print(f"✅ SMART CAPTURE COMPLETE:")
        print(f"   📧 Emails captured for fallback: {len(emails_found)}")
        print(f"   👤 Contacts captured for fallback: {len(contact_info)}")
        
        return all_staff, all_social

    def _analyze_single_url_with_content_capture(self, url: str) -> tuple:
        """🔍 Analyze single URL and return content for fallback analysis"""
        
        # JavaScript function that captures MORE content
        page_function = """
        async function pageFunction(context) {
            const { request, log, jQuery } = context;
            const $ = jQuery;
            
            await context.waitFor(8000);
            
            try {
                const bodyText = $('body').text() || '';
                const socialMedia = {};
                
                $('a[href]').each(function() {
                    const href = $(this).attr('href');
                    if (!href) return;
                    
                    const lowerHref = href.toLowerCase();
                    if (lowerHref.includes('linkedin.com') && !socialMedia.linkedin) {
                        socialMedia.linkedin = href;
                    } else if (lowerHref.includes('facebook.com') && !socialMedia.facebook) {
                        socialMedia.facebook = href;
                    } else if (lowerHref.includes('twitter.com') && !socialMedia.twitter) {
                        socialMedia.twitter = href;
                    } else if (lowerHref.includes('instagram.com') && !socialMedia.instagram) {
                        socialMedia.instagram = href;
                    }
                });
                
                return {
                    url: request.url,
                    content: bodyText,
                    socialMedia: socialMedia
                };
                
            } catch (error) {
                return {
                    url: request.url,
                    content: '',
                    socialMedia: {}
                };
            }
        }
        """
        
        payload = {
            "startUrls": [{"url": url}],
            "maxPagesPerRun": 1,
            "pageFunction": page_function,
            "proxyConfiguration": {"useApifyProxy": True}
        }
        
        try:
            client = get_working_apify_client_part1()
            run = client.actor("apify~web-scraper").call(run_input=payload)
            
            if run:
                items = client.dataset(run["defaultDatasetId"]).list_items().items
                if items:
                    content = items[0].get('content', '')
                    social = items[0].get('socialMedia', {})
                    
                    # Analyze content with GPT-4o for staff
                    staff = self._gpt_extract_staff(content, url)
                    
                    # 🧠 SMART: Return content for fallback analysis
                    return staff, social, content
            
            return [], {}, ""
            
        except Exception as e:
            print(f"    ❌ Content analysis failed: {e}")
            return [], {}, ""

    def _extract_emails_from_content(self, content: str) -> list:
        """📧 Extract email addresses directly from website content"""
        
        if not content:
            return []
        
        print("🔍 Extracting emails from captured content...")
        
        # Email regex patterns
        email_patterns = [
            r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            r'\b[A-Za-z0-9._%+-]+\s*@\s*[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            r'mailto:([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,})',
        ]
        
        found_emails = set()
        
        for pattern in email_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):
                    email = match[0]  # For mailto: pattern
                else:
                    email = match
                
                # Clean and validate email
                email = email.strip().lower()
                if self._is_valid_business_email(email):
                    found_emails.add(email)
                    print(f"   📧 Captured email: {email}")
        
        return list(found_emails)

    def _is_valid_business_email(self, email: str) -> bool:
        """Validate if email looks like a business email"""
        
        # Skip obvious spam/fake emails
        spam_keywords = ['noreply', 'no-reply', 'donotreply', 'example', 'test', 'spam']
        if any(keyword in email.lower() for keyword in spam_keywords):
            return False
        
        # Basic email format validation
        if '@' not in email or '.' not in email.split('@')[1]:
            return False
        
        return True

    def _extract_contact_info_gpt(self, content: str, domain: str) -> list:
        """🧠 Use GPT-4o-mini to extract contact information from website content"""
        
        if len(content) < 1000:
            print("⚠️ Insufficient content for GPT analysis")
            return []
        
        prompt = f"""Analyze this website content and extract ALL possible contact information for business decision-makers.

WEBSITE: {domain}

EXTRACT:
1. Names of people (owners, managers, directors, key staff)
2. Job titles/roles
3. Phone numbers
4. Any contact information
5. Department heads or key contacts

RULES:
- Focus on decision-makers who would handle fire safety/building compliance
- Include owners, managers, facilities managers, operations staff
- Exclude obvious clients, testimonials, or external people
- Include anyone who might make purchasing decisions

Return as JSON:
[
  {{
    "name": "Full Name",
    "title": "Job Title", 
    "phone": "Phone if found",
    "department": "Department if mentioned",
    "role_type": "owner/manager/director/staff",
    "fire_protection_relevance": "high/medium/low"
  }}
]

If no contacts found: []

CONTENT:
{content[:150000]}"""  # Use substantial content for analysis

        try:
            from openai import OpenAI
            client = OpenAI(api_key=self.openai_key)
            
            response = client.chat.completions.create(
                model="gpt-4o-mini",  # Cost-effective for this analysis
                messages=[{"role": "user", "content": prompt}],
                max_tokens=2000,
                temperature=0.1
            )
            
            result_text = response.choices[0].message.content.strip()
            
            print(f"\n🧠 GPT-4o-mini CONTACT ANALYSIS:")
            print("-" * 60)
            print(result_text[:500] + "..." if len(result_text) > 500 else result_text)
            print("-" * 60)
            
            # Parse JSON
            json_start = result_text.find('[')
            json_end = result_text.rfind(']') + 1
            
            if json_start >= 0 and json_end > json_start:
                json_text = result_text[json_start:json_end]
                contacts = json.loads(json_text)
                
                print(f"✅ GPT-4o-mini captured {len(contacts)} potential contacts")
                for contact in contacts:
                    name = contact.get('name', 'Unknown')
                    title = contact.get('title', 'Unknown')
                    relevance = contact.get('fire_protection_relevance', 'unknown')
                    print(f"   👤 {name} - {title} (Relevance: {relevance})")
                
                return contacts
                
        except Exception as e:
            print(f"⚠️ GPT contact extraction failed: {e}")
        
        return []

    def _generate_emails_for_captured_contacts(self, contact_info: list, found_emails: list, domain: str) -> list:
        """🧪 Generate and verify emails for captured contacts"""
        
        from account_manager import MillionVerifierManager
        millionverifier = MillionVerifierManager()
        
        verified_contacts = []
        
        # First, add any directly found emails as contacts
        for email in found_emails:
            contact = {
                'name': self._extract_name_from_email(email),
                'title': 'Contact Person',
                'email': email,
                'email_source': 'direct_extraction',
                'verification_status': 'found_on_website',
                'fire_protection_relevance': 'medium'
            }
            verified_contacts.append(contact)
            print(f"   📧 Direct email added: {email}")
        
        # Then, generate emails for named contacts
        for contact in contact_info:
            name = contact.get('name', '')
            if not name or len(name.split()) < 2:
                continue
            
            name_parts = name.split()
            first_name = name_parts[0]
            last_name = name_parts[-1]
            middle_name = " ".join(name_parts[1:-1]) if len(name_parts) > 2 else ""
            
            print(f"\n🧪 Generating emails for captured contact: {name}")
            
            # Generate common email patterns
            email_patterns = self._generate_common_email_patterns(first_name, last_name, domain, middle_name)
            
            email_found = False
            for i, test_email in enumerate(email_patterns, 1):
                print(f"   🔍 Testing pattern {i}/{len(email_patterns)}: {test_email}")
                
                if millionverifier.smart_verify_email(test_email, domain):
                    print(f"   ✅ EMAIL VERIFIED: {test_email}")
                    
                    contact['email'] = test_email
                    contact['email_source'] = f'generated_pattern_{i}'
                    contact['verification_status'] = 'verified'
                    verified_contacts.append(contact)
                    email_found = True
                    break
                else:
                    print(f"   ❌ Pattern failed: {test_email}")
            
            if not email_found:
                print(f"   😞 No valid email found for {name}")
        
        return verified_contacts

    def _generate_common_email_patterns(self, first_name: str, last_name: str, domain: str, middle_name: str = "") -> list:
        """Generate common email patterns for business contacts"""
        
        first = first_name.lower().strip()
        last = last_name.lower().strip()
        f = first[0] if first else ''
        l = last[0] if last else ''
        
        patterns = [
            f"{first}@{domain}",
            f"{last}@{domain}",
            f"{first}.{last}@{domain}",
            f"{f}{last}@{domain}",
            f"{first}{last}@{domain}",
            f"{f}.{last}@{domain}",
            f"{last}.{first}@{domain}",
            f"{first}_{last}@{domain}",
            f"{first}-{last}@{domain}",
            f"{first}{l}@{domain}",
            f"{f}{l}@{domain}"
        ]
        
        # Remove duplicates
        return list(dict.fromkeys(patterns))

    def _extract_name_from_email(self, email: str) -> str:
        """Extract a name from an email address"""
        local_part = email.split('@')[0]
        
        # Handle common separators
        if '.' in local_part:
            parts = local_part.split('.')
            return ' '.join(part.capitalize() for part in parts if len(part) > 1)
        elif '_' in local_part:
            parts = local_part.split('_')
            return ' '.join(part.capitalize() for part in parts if len(part) > 1)
        elif '-' in local_part:
            parts = local_part.split('-')
            return ' '.join(part.capitalize() for part in parts if len(part) > 1)
        else:
            return local_part.capitalize()

    def _score_website_contacts_fire_protection(self, contacts: list) -> list:
        """🔥 Score website contacts for fire protection relevance"""
        
        for contact in contacts:
            title = contact.get('title', '').lower()
            name = contact.get('name', '').lower()
            role_type = contact.get('role_type', '').lower()
            existing_relevance = contact.get('fire_protection_relevance', 'low')
            
            score = 0
            reason = 'General contact'
            
            # Score based on role type
            if role_type in ['owner', 'director']:
                score += 80
                reason = 'Business owner/director - ultimate responsibility for compliance'
            elif role_type in ['manager']:
                score += 70
                reason = 'Management role - budget authority for safety investments'
            
            # Score based on title keywords
            if any(keyword in title for keyword in ['facilities', 'operations', 'maintenance']):
                score += 100
                reason = 'Facilities/Operations role - direct building responsibility'
            elif any(keyword in title for keyword in ['safety', 'compliance', 'risk']):
                score += 100
                reason = 'Safety role - direct fire protection responsibility'
            elif any(keyword in title for keyword in ['owner', 'director', 'ceo', 'md']):
                score += 80
                reason = 'Senior leadership - compliance responsibility'
            elif any(keyword in title for keyword in ['manager']):
                score += 60
                reason = 'Management role - safety decision maker'
            
            # Boost score based on existing GPT relevance assessment
            if existing_relevance == 'high':
                score += 30
            elif existing_relevance == 'medium':
                score += 15
            
            contact['fire_protection_score'] = score
            contact['fire_protection_reason'] = reason
            
            print(f"   🔥 {contact['name']} - Score: {score} | {reason}")
        
        # Sort by score and return top contacts
        contacts.sort(key=lambda x: x.get('fire_protection_score', 0), reverse=True)
        return contacts

    # Keep existing methods for regular LinkedIn flow
    def _stealth_website_mapping(self, url: str) -> dict:
        """🗺️ Stealth website mapping using Apify with account rotation"""
        
        mapping_function = """
async function pageFunction(context) {
    const { request, log, jQuery } = context;
    const $ = jQuery;
    
    await context.waitFor(5000);
    
    try {
        const allLinks = [];
        $('a[href]').each(function() {
            const href = $(this).attr('href');
            const text = $(this).text().trim();
            
            if (href && href.length > 1) {
                let fullUrl = href;
                if (href.startsWith('/')) {
                    const baseUrl = request.url.split('/').slice(0, 3).join('/');
                    fullUrl = baseUrl + href;
                }
                
                allLinks.push({
                    url: fullUrl,
                    text: text,
                    href: href
                });
            }
        });
        
        return {
            url: request.url,
            websiteMap: {
                allLinks: allLinks,
                domain: request.url.split('/')[2]
            }
        };
        
    } catch (error) {
        return {
            url: request.url,
            websiteMap: { allLinks: [], domain: request.url.split('/')[2] }
        };
    }
}
"""
        
        payload = {
            "startUrls": [{"url": url}],
            "maxPagesPerRun": 1,
            "pageFunction": mapping_function,
            "proxyConfiguration": {"useApifyProxy": True}
        }
        
        try:
            client = get_working_apify_client_part1()
            run = client.actor("apify~web-scraper").call(run_input=payload)
            
            if run:
                items = client.dataset(run["defaultDatasetId"]).list_items().items
                if items:
                    return items[0].get('websiteMap', {})
            
            return {}
            
        except Exception as e:
            print(f"❌ Website mapping failed: {e}")
            return {}

    def _gpt_analyze_urls(self, website_map: dict, domain: str) -> list:
        """🧠 GPT-4o analyzes URLs to select best ones for staff extraction"""
        
        all_links = website_map.get('allLinks', [])
        
        # Create link summary for GPT
        link_summary = []
        for link in all_links[:50]:  # Top 50 to avoid token limits
            url = link.get('url', '')
            text = link.get('text', '')
            href = link.get('href', '')
            link_summary.append(f"{text} → {href}")
        
        prompt = f"""Analyze this website and select the 3 BEST URLs for finding staff information.

DOMAIN: {domain}
AVAILABLE LINKS:
{chr(10).join(link_summary)}

Select URLs most likely to contain staff/team information. Return as JSON:
{{
  "selected_urls": [
    "full_url_1",
    "full_url_2", 
    "full_url_3"
  ]
}}

Focus on: about, team, staff, people, contact, leadership pages."""

        try:
            from openai import OpenAI
            client = OpenAI(api_key=self.openai_key)
            
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=500,
                temperature=0.1
            )
            
            result_text = response.choices[0].message.content.strip()
            
            # Parse JSON
            json_start = result_text.find('{')
            json_end = result_text.rfind('}') + 1
            
            if json_start >= 0 and json_end > json_start:
                json_text = result_text[json_start:json_end]
                analysis = json.loads(json_text)
                urls = analysis.get('selected_urls', [])
                
                print(f"🎯 GPT-4o selected {len(urls)} URLs:")
                for i, url in enumerate(urls, 1):
                    print(f"   {i}. {url}")
                
                return urls
                
        except Exception as e:
            print(f"⚠️ GPT analysis failed: {e}")
        
        # Fallback: keyword-based selection
        staff_keywords = ["/team", "/about", "/staff", "/people", "/contact"]
        selected = []
        
        for link in all_links:
            url = link.get('url', '').lower()
            if any(keyword in url for keyword in staff_keywords):
                selected.append(link.get('url', ''))
                if len(selected) >= 3:
                    break
        
        return selected

    def _gpt_extract_staff(self, content: str, url: str) -> list:
        """🧠 ENHANCED: GPT-4o extracts staff from content with better content analysis"""
        
        # 🔧 FIXED: Higher minimum content threshold
        if len(content) < 2000:  # Increased from 500 to 2000
            return []
        
        domain = urlparse(url).netloc.replace('www.', '')
        
        # 🔥 NEW: Extract staff-related sections first
        staff_content = self._extract_staff_sections(content)
        if staff_content:
            content = staff_content
        
        prompt = f"""Extract staff from this {domain} content.

RULES:
1. Find ONLY current employees of {domain}
2. Need: Full name + Job title
3. EXCLUDE: Clients, testimonials, external people
4. EXCLUDE: Company names mistaken as people
5. Require: First name + Last name (minimum)

Return JSON: [{{"name": "Full Name", "title": "Job Title"}}]
If none found: []

Content:
{content[:100000]}"""  # 🔧 FIXED: Increased from 15,000 to 100,000 characters

        try:
            from openai import OpenAI
            client = OpenAI(api_key=self.openai_key)
            
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=1500,  # Increased for better results
                temperature=0.1
            )
            
            result_text = response.choices[0].message.content.strip()
            
            print(f"\n🧠 GPT-4o RAW RESPONSE:")
            print("-" * 50)
            print(result_text)
            print("-" * 50)
            
            # Parse JSON
            json_start = result_text.find('[')
            json_end = result_text.rfind(']') + 1
            
            if json_start >= 0 and json_end > json_start:
                json_text = result_text[json_start:json_end]
                staff_list = json.loads(json_text)
                
                # 🔥 NEW: Better staff validation
                validated_staff = []
                for staff in staff_list:
                    name = staff.get('name', '')
                    title = staff.get('title', '')
                    
                    # Validate name structure
                    if self._validate_staff_name(name, domain):
                        validated_staff.append(staff)
                        print(f"✅ Valid staff: {name} - {title}")
                    else:
                        print(f"❌ Invalid staff: {name} - {title}")
                
                return validated_staff[:10]  # Limit results
                
        except Exception as e:
            print(f"    ⚠️ GPT extraction failed: {e}")
        
        return []
    
    def _extract_staff_sections(self, content: str) -> str:
        """🔥 NEW: Extract staff-related sections from content"""
        staff_keywords = [
            'about us', 'team', 'staff', 'our people', 'leadership',
            'meet the team', 'our team', 'employees', 'directors'
        ]
        
        # Simple section extraction based on keywords
        lines = content.split('\n')
        staff_lines = []
        in_staff_section = False
        
        for line in lines:
            line_lower = line.lower().strip()
            
            # Check if we're entering a staff section
            if any(keyword in line_lower for keyword in staff_keywords):
                in_staff_section = True
                staff_lines.append(line)
                continue
            
            # If in staff section, keep collecting lines
            if in_staff_section:
                staff_lines.append(line)
                
                # Stop if we hit a clear section break
                if len(line.strip()) == 0 and len(staff_lines) > 20:
                    break
        
        return '\n'.join(staff_lines) if staff_lines else content
    
    def _validate_staff_name(self, name: str, domain: str) -> bool:
        """🔥 NEW: Validate if name looks like a real person"""
        if not name or len(name.strip()) < 3:
            return False
        
        name_parts = name.strip().split()
        
        # Must have at least first and last name
        if len(name_parts) < 2:
            return False
        
        # Check against company name
        company_name = domain.replace('.com', '').replace('.co.uk', '').replace('.ie', '')
        if name.lower().replace(' ', '') == company_name.lower().replace(' ', ''):
            return False
        
        # Reject obvious non-person names
        reject_keywords = [
            'company', 'ltd', 'limited', 'inc', 'corp', 'team', 'department',
            'marketing', 'sales', 'support', 'admin', 'office'
        ]
        
        if any(keyword in name.lower() for keyword in reject_keywords):
            return False
        
        return True

    def _normalize_www(self, url: str) -> str:
        """🌐 Normalize URL to include www"""
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        parsed = urlparse(url)
        netloc = parsed.netloc
        
        if netloc and not netloc.startswith('www.'):
            netloc = 'www.' + netloc
        
        return urlunparse((parsed.scheme, netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))
