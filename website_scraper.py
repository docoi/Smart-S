"""
🌐 Website Scraper Module
========================
Handles website scraping, staff extraction, LinkedIn URL discovery, and Smart Fallback
✅ UPDATED: Now includes Smart Fallback functionality for when LinkedIn is unavailable
"""

import json
import re
from urllib.parse import urlparse, urlunparse
from account_manager import get_working_apify_client_part1


class WebsiteScraper:
    """🌐 Website scraping with GPT-4o analysis and Smart Fallback"""
    
    def __init__(self, openai_key):
        self.openai_key = openai_key
    
    def scrape_website_for_staff_and_linkedin(self, website_url: str) -> dict:
        """📋 Main method: Scrape website for staff, LinkedIn URL, and Smart Fallback data"""
        
        print(f"🗺️ Phase 1a: Website mapping...")
        
        # Normalize URL
        normalized_url = self._normalize_www(website_url)
        
        # Map website to find URLs using account rotation
        website_map = self._stealth_website_mapping(normalized_url)
        
        if not website_map:
            print("❌ Website mapping failed")
            return {'staff': [], 'linkedin_url': '', 'fallback_contacts': [], 'fallback_emails': []}
        
        print(f"🧠 Phase 1b: GPT-4o URL analysis...")
        
        # Analyze with GPT-4o to select best URLs
        selected_urls = self._gpt_analyze_urls(website_map, normalized_url)
        
        if not selected_urls:
            print("❌ No URLs selected for analysis")
            return {'staff': [], 'linkedin_url': '', 'fallback_contacts': [], 'fallback_emails': []}
        
        print(f"🔍 Phase 1c: SMART content analysis - capturing ALL data...")
        
        # Extract staff and LinkedIn from selected URLs using account rotation
        staff_list, social_data, all_content = self._extract_staff_and_social_with_content(selected_urls)
        
        # Find LinkedIn URL
        linkedin_url = (social_data.get('company_linkedin') or 
                       social_data.get('linkedin') or 
                       social_data.get('LinkedIn') or "")
        
        # 🔥 NEW: Smart Fallback - Extract emails and contacts for fallback
        fallback_emails, fallback_contacts = self._smart_fallback_extraction(all_content, normalized_url)
        
        print(f"✅ Website scraping complete:")
        print(f"   👥 Staff found: {len(staff_list)}")
        if linkedin_url:
            print(f"   🔗 LinkedIn URL: {linkedin_url[:50]}...")
        else:
            print(f"   ❌ No LinkedIn URL found")
        print(f"   📧 Fallback emails captured: {len(fallback_emails)}")
        print(f"   👤 Fallback contacts captured: {len(fallback_contacts)}")
        print(f"   💾 Fallback data ready for later use if needed")
        
        return {
            'staff': staff_list,
            'linkedin_url': linkedin_url,
            'fallback_contacts': fallback_contacts,
            'fallback_emails': fallback_emails
        }

    def _extract_staff_and_social_with_content(self, urls: list) -> tuple:
        """🔍 Extract staff, social media, and content from selected URLs"""
        
        all_staff = []
        all_social = {}
        all_content = ""
        
        for i, url in enumerate(urls, 1):
            print(f"  📄 Processing {i}/{len(urls)}: {url}")
            
            try:
                staff, social, content = self._analyze_single_url_with_content(url)
                all_staff.extend(staff)
                all_social.update(social)
                all_content += content + "\n\n"
                
                if staff:
                    print(f"    ✅ Found {len(staff)} staff members")
                
            except Exception as e:
                print(f"    ⚠️ Error: {e}")
                
        return all_staff, all_social, all_content

    def _analyze_single_url_with_content(self, url: str) -> tuple:
        """🔍 Analyze single URL for staff, social media, and capture content"""
        
        # Enhanced JavaScript function to capture more data
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
            # Get client with credit-based account rotation for Part 1
            client = get_working_apify_client_part1()
            
            run = client.actor("apify~web-scraper").call(run_input=payload)
            
            if run:
                items = client.dataset(run["defaultDatasetId"]).list_items().items
                if items:
                    content = items[0].get('content', '')
                    social = items[0].get('socialMedia', {})
                    
                    # Analyze content with GPT-4o for staff
                    staff = self._gpt_extract_staff(content, url)
                    
                    return staff, social, content
            
            return [], {}, ""
            
        except Exception as e:
            print(f"    ❌ Content analysis failed: {e}")
            return [], {}, ""

    def _smart_fallback_extraction(self, all_content: str, website_url: str) -> tuple:
        """🔄 Smart Fallback: Extract emails and contacts from captured content"""
        
        print(f"\n🧠 SMART ANALYSIS: Extracting fallback data from {len(all_content)} characters")
        
        # Extract emails using regex
        fallback_emails = self._extract_emails_from_content(all_content, website_url)
        
        # Extract contacts using GPT-4o-mini
        fallback_contacts = self._extract_contacts_gpt(all_content, website_url)
        
        print(f"✅ SMART CAPTURE COMPLETE:")
        print(f"   📧 Emails captured for fallback: {len(set(fallback_emails))}")
        print(f"   👤 Contacts captured for fallback: {len(fallback_contacts)}")
        
        # Remove duplicates from emails
        unique_emails = list(set(fallback_emails))
        
        return unique_emails, fallback_contacts

    def _extract_emails_from_content(self, content: str, website_url: str) -> list:
        """📧 Extract email addresses from content"""
        
        print(f"🔍 Extracting emails from captured content...")
        
        domain = urlparse(website_url).netloc.replace('www.', '')
        
        # Email regex pattern
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        emails = re.findall(email_pattern, content)
        
        # Filter for relevant emails (same domain preferred)
        relevant_emails = []
        other_emails = []
        
        for email in emails:
            email = email.lower().strip()
            if domain in email:
                relevant_emails.append(email)
                print(f"   📧 Captured email: {email}")
            else:
                other_emails.append(email)
        
        # Prefer domain emails, but include others if needed
        final_emails = relevant_emails + other_emails[:3]  # Limit other emails
        
        return final_emails

    def _extract_contacts_gpt(self, content: str, website_url: str) -> list:
        """👤 Extract contact information using GPT-4o-mini"""
        
        domain = urlparse(website_url).netloc.replace('www.', '')
        
        # Limit content to avoid token limits
        limited_content = content[:50000]  # 50K chars should be safe
        
        prompt = f"""Extract key business contacts from this website content for {domain}.

IMPORTANT: Find people who would be responsible for fire safety, building management, or business operations.

Look for:
- Names with job titles
- Phone numbers associated with people
- People in management, operations, technical, or director roles
- Department heads or facility managers

Return as JSON array:
[
  {{
    "name": "Full Name",
    "title": "Job Title",
    "phone": "Phone Number (if found)",
    "department": "Department (if found)",
    "role_type": "manager/director/technical/operations",
    "fire_protection_relevance": "high/medium/low"
  }}
]

If no clear contacts found, return []

Content:
{limited_content}"""

        try:
            from openai import OpenAI
            client = OpenAI(api_key=self.openai_key)
            
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=1000,
                temperature=0.1
            )
            
            result_text = response.choices[0].message.content.strip()
            
            print(f"\n🧠 GPT-4o-mini CONTACT ANALYSIS:")
            print("-" * 60)
            print(result_text)
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
                    title = contact.get('title', 'Unknown Role')
                    relevance = contact.get('fire_protection_relevance', 'medium')
                    print(f"   👤 {name} - {title} (Relevance: {relevance})")
                
                return contacts
                
        except Exception as e:
            print(f"   ⚠️ GPT contact extraction failed: {e}")
        
        return []

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
            # Get client with credit-based account rotation for Part 1
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
