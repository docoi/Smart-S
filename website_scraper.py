"""
🌐 Website Scraper Module - COMPLETE FIXED VERSION
========================
Handles website scraping, staff extraction, LinkedIn URL discovery, and Smart Fallback
✅ FIXED: LinkedIn URL extraction with GPT-4o social media finder
✅ FIXED: Smart Fallback functionality for when LinkedIn fails
✅ FIXED: Returns dict format for compatibility with main.py
✅ FIXED: Email extraction and cleaning
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
        """📋 Main method: Scrape website for staff and LinkedIn URL with Smart Fallback"""
        
        print(f"🗺️ Phase 1a: Website mapping...")
        
        # Normalize URL
        normalized_url = self._normalize_www(website_url)
        domain = urlparse(normalized_url).netloc.replace('www.', '')
        
        # Map website to find URLs using account rotation
        website_map = self._stealth_website_mapping(normalized_url)
        
        if not website_map:
            print("❌ Website mapping failed")
            return self._create_empty_result(normalized_url)
        
        print(f"🧠 Phase 1b: GPT-4o URL analysis...")
        
        # Analyze with GPT-4o to select best URLs
        selected_urls = self._gpt_analyze_urls(website_map, normalized_url)
        
        if not selected_urls:
            print("❌ No URLs selected for analysis")
            return self._create_empty_result(normalized_url)
        
        print(f"🔍 Phase 1c: Content analysis...")
        
        # Extract staff and social media from selected URLs
        staff_list, social_data, all_content = self._extract_staff_and_social(selected_urls)
        
        # Enhanced LinkedIn URL discovery with GPT-4o
        linkedin_url = self._discover_linkedin_url_gpt(social_data, all_content, domain)
        
        # Smart Fallback data preparation
        fallback_emails = self._extract_emails_from_content(all_content, domain)
        fallback_contacts = self._prepare_fallback_contacts(staff_list, fallback_emails, domain)
        
        print(f"✅ Website scraping complete:")
        print(f"   👥 Staff found: {len(staff_list)}")
        print(f"   📧 Fallback emails: {len(fallback_emails)}")
        print(f"   👤 Fallback contacts: {len(fallback_contacts)}")
        print(f"   🔗 LinkedIn URL: {linkedin_url[:50]}..." if linkedin_url else "   ❌ No LinkedIn URL found")
        
        return {
            'status': 'completed',
            'website_staff': staff_list,
            'linkedin_url': linkedin_url,
            'fallback_emails': fallback_emails,
            'fallback_contacts': fallback_contacts,
            'domain': domain,
            'normalized_url': normalized_url
        }

    def _create_empty_result(self, url: str) -> dict:
        """Create empty result structure"""
        domain = urlparse(url).netloc.replace('www.', '')
        return {
            'status': 'failed',
            'website_staff': [],
            'linkedin_url': '',
            'fallback_emails': [],
            'fallback_contacts': [],
            'domain': domain,
            'normalized_url': url
        }

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
            "pageFunction": mapping_function,  # ✅ FIXED: correct variable name
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

    def _extract_staff_and_social(self, urls: list) -> tuple:
        """🔍 Extract staff, social media, and content from selected URLs"""
        
        all_staff = []
        all_social = {}
        all_content = ""
        
        for i, url in enumerate(urls, 1):
            print(f"  📄 Processing {i}/{len(urls)}: {url}")
            
            try:
                staff, social, content = self._analyze_single_url(url)
                all_staff.extend(staff)
                all_social.update(social)
                all_content += f"\n\n=== CONTENT FROM {url} ===\n{content}"
                
                if staff:
                    print(f"    ✅ Found {len(staff)} staff members")
                
            except Exception as e:
                print(f"    ⚠️ Error: {e}")
                
        return all_staff, all_social, all_content

    def _analyze_single_url(self, url: str) -> tuple:
        """🔍 Analyze single URL for staff, social media, and content"""
        
        page_function = """
        async function pageFunction(context) {
            const { request, log, jQuery } = context;
            const $ = jQuery;
            
            await context.waitFor(8000);
            
            try {
                const bodyText = $('body').text() || '';
                const bodyHtml = $('body').html() || '';
                const socialMedia = {};
                
                // Enhanced social media extraction
                $('a[href]').each(function() {
                    const href = $(this).attr('href');
                    if (!href) return;
                    
                    const lowerHref = href.toLowerCase();
                    
                    // LinkedIn - prioritize company pages
                    if (lowerHref.includes('linkedin.com')) {
                        if (lowerHref.includes('/company/') && !socialMedia.company_linkedin) {
                            socialMedia.company_linkedin = href;
                        } else if (!socialMedia.linkedin) {
                            socialMedia.linkedin = href;
                        }
                    }
                    // Other social media
                    else if (lowerHref.includes('facebook.com') && !socialMedia.facebook) {
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
                    html_content: bodyHtml,
                    socialMedia: socialMedia
                };
                
            } catch (error) {
                return {
                    url: request.url,
                    content: '',
                    html_content: '',
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
                    html_content = items[0].get('html_content', '')
                    social = items[0].get('socialMedia', {})
                    
                    # Analyze content with GPT-4o for staff
                    staff = self._gpt_extract_staff(content, url)
                    
                    # Combine text and HTML content for comprehensive analysis
                    full_content = f"{content}\n\nHTML_CONTENT:\n{html_content}"
                    
                    return staff, social, full_content
            
            return [], {}, ""
            
        except Exception as e:
            print(f"    ❌ Content analysis failed: {e}")
            return [], {}, ""

    def _discover_linkedin_url_gpt(self, social_data: dict, all_content: str, domain: str) -> str:
        """🧠 Enhanced LinkedIn URL discovery using GPT-4o as backup"""
        
        # First, try the extracted social media data and clean it
        linkedin_url = (social_data.get('company_linkedin') or 
                       social_data.get('linkedin') or 
                       social_data.get('LinkedIn') or "")
        
        if linkedin_url:
            cleaned_url = self._clean_linkedin_url(linkedin_url)
            if self._validate_linkedin_url(cleaned_url, domain):
                print(f"✅ LinkedIn URL found from social extraction: {cleaned_url}")
                return cleaned_url
        
        # If no valid URL found, use GPT-4o to find it from content
        print("🧠 Using GPT-4o to find LinkedIn URL from content...")
        
        try:
            from openai import OpenAI
            client = OpenAI(api_key=self.openai_key)
            
            # Extract relevant parts of content for LinkedIn search
            content_sample = all_content[:20000]  # First 20k chars to avoid token limits
            
            prompt = f"""Find the correct LinkedIn company URL for {domain} from this website content.

DOMAIN: {domain}
CONTENT: {content_sample}

RULES:
1. Look for LinkedIn URLs that contain '/company/' (these are company pages)
2. The URL should be for {domain} specifically
3. Ignore personal LinkedIn profiles (they contain '/in/')
4. Return the complete, clean URL in the standard format

If found, return ONLY the URL in this format:
https://www.linkedin.com/company/company-name

If not found, return: NOT_FOUND"""

            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=100,
                temperature=0.1
            )
            
            result = response.choices[0].message.content.strip()
            
            if result != "NOT_FOUND" and "linkedin.com/company/" in result.lower():
                cleaned_url = self._clean_linkedin_url(result)
                if self._validate_linkedin_url(cleaned_url, domain):
                    print(f"✅ LinkedIn URL found by GPT-4o: {cleaned_url}")
                    return cleaned_url
            
            print("❌ GPT-4o could not find valid LinkedIn company URL")
            return ""
            
        except Exception as e:
            print(f"⚠️ GPT-4o LinkedIn discovery failed: {e}")
            return ""

    def _validate_linkedin_url(self, url: str, domain: str) -> bool:
        """✅ Validate LinkedIn URL format and relevance"""
        if not url:
            return False
            
        # Clean and normalize URL
        url = url.strip().rstrip('/')
        
        # Must be a LinkedIn company page
        if "/company/" not in url.lower():
            return False
            
        # Must be HTTPS
        if not url.startswith("https://"):
            return False
            
        # Should not contain tracking parameters or invalid paths
        invalid_paths = ["/top-content", "/people", "/posts", "/jobs", "/life"]
        if any(path in url.lower() for path in invalid_paths):
            return False
            
        return True

    def _clean_linkedin_url(self, url: str) -> str:
        """🧽 Clean and normalize LinkedIn URL"""
        if not url:
            return ""
            
        # Remove tracking parameters
        url = url.split('?')[0].split('#')[0]
        
        # Ensure HTTPS
        if url.startswith("http://"):
            url = url.replace("http://", "https://")
        elif not url.startswith("https://"):
            url = "https://" + url
            
        # Ensure proper LinkedIn domain (convert uk.linkedin.com to www.linkedin.com)
        if "uk.linkedin.com" in url:
            url = url.replace("uk.linkedin.com", "www.linkedin.com")
        elif "linkedin.com" in url and "www.linkedin.com" not in url:
            url = url.replace("linkedin.com", "www.linkedin.com")
            
        # Remove trailing slash
        url = url.rstrip('/')
        
        return url

    def _extract_emails_from_content(self, content: str, domain: str) -> list:
        """📧 Extract and clean emails from website content"""
        
        if not content:
            return []
            
        # Enhanced email regex pattern
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        
        # Find all potential emails
        potential_emails = re.findall(email_pattern, content)
        
        # Filter and clean emails
        domain_emails = []
        for email in potential_emails:
            email = email.lower().strip()
            
            # Only keep emails from the target domain
            if f"@{domain}" in email:
                # Clean email - remove any trailing garbage
                email = re.sub(r'[.,;:!?"\'>)\]}\s]*$', '', email)
                
                # Remove common garbage patterns that get attached
                email = re.sub(r'(contact|info|admin|support|mail|email)\w*$', '', email)
                email = re.sub(r'\d+.*$', '', email)  # Remove trailing numbers and text
                
                # Validate email format after cleaning
                if re.match(r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$', email):
                    if email not in domain_emails and len(email) > 5:
                        domain_emails.append(email)
        
        print(f"📧 Found {len(domain_emails)} emails from {domain}: {domain_emails}")
        return domain_emails

    def _prepare_fallback_contacts(self, staff_list: list, emails: list, domain: str) -> list:
        """👤 Prepare fallback contacts by matching staff to emails"""
        
        fallback_contacts = []
        
        # Create a copy of emails for matching
        available_emails = emails.copy()
        
        for staff in staff_list:
            name = staff.get('name', '')
            title = staff.get('title', '')
            
            if not name:
                continue
                
            # Try to match staff to emails
            matched_email = self._match_staff_to_email(name, available_emails, domain)
            
            contact = {
                'name': name,
                'title': title,
                'email': matched_email if matched_email else '',
                'source': 'website_fallback',
                'priority': self._determine_priority_score(title),
                'fire_protection_score': 0,  # Will be calculated later
                'fire_protection_reason': '',
                'email_verified': False
            }
            
            fallback_contacts.append(contact)
            
            # Remove used email from available pool
            if matched_email and matched_email in available_emails:
                available_emails.remove(matched_email)
        
        # Add any remaining emails as generic contacts
        for email in available_emails:
            contact = {
                'name': self._extract_name_from_email(email),
                'title': 'Contact',
                'email': email,
                'source': 'website_email',
                'priority': 50,
                'fire_protection_score': 0,
                'fire_protection_reason': '',
                'email_verified': False
            }
            fallback_contacts.append(contact)
        
        return fallback_contacts

    def _match_staff_to_email(self, name: str, emails: list, domain: str) -> str:
        """🎯 Smart matching of staff names to email addresses"""
        
        if not name or not emails:
            return ""
            
        # Parse name
        name_parts = name.lower().split()
        if len(name_parts) < 2:
            return ""
            
        first_name = name_parts[0]
        last_name = name_parts[-1]
        
        # Try exact matches first
        for email in emails:
            local_part = email.split('@')[0].lower()
            
            # Try various common patterns
            patterns = [
                first_name,
                last_name,
                f"{first_name}.{last_name}",
                f"{first_name}_{last_name}",
                f"{first_name}-{last_name}",
                f"{first_name[0]}{last_name}",
                f"{first_name}{last_name}",
                f"{last_name}{first_name}",
                f"{first_name[0]}.{last_name}"
            ]
            
            if local_part in patterns:
                return email
        
        # If no exact match, return the first available email
        return emails[0] if emails else ""

    def _extract_name_from_email(self, email: str) -> str:
        """📧 Extract likely name from email address"""
        
        try:
            local_part = email.split('@')[0]
            
            # Replace common separators with spaces
            name = local_part.replace('.', ' ').replace('_', ' ').replace('-', ' ')
            
            # Skip generic email addresses
            generic_prefixes = ['info', 'contact', 'admin', 'support', 'sales', 'hello', 'mail']
            if any(name.startswith(prefix) for prefix in generic_prefixes):
                return "Contact Person"
            
            # Capitalize each word
            name = ' '.join(word.capitalize() for word in name.split())
            
            return name if len(name) > 2 else "Contact Person"
            
        except:
            return "Contact Person"

    def _determine_priority_score(self, title: str) -> int:
        """🎯 Determine priority score based on job title (higher = more important)"""
        
        if not title:
            return 30
            
        title_lower = title.lower()
        
        # Highest priority - Senior leadership
        if any(keyword in title_lower for keyword in ['ceo', 'owner', 'founder', 'director', 'managing']):
            return 90
            
        # High priority - Management
        if any(keyword in title_lower for keyword in ['manager', 'head', 'lead', 'supervisor']):
            return 80
            
        # Medium-high priority - Specialized roles
        if any(keyword in title_lower for keyword in ['facilities', 'operations', 'safety', 'coordinator']):
            return 70
            
        # Medium priority - Professional roles
        if any(keyword in title_lower for keyword in ['specialist', 'analyst', 'consultant', 'officer']):
            return 60
            
        # Lower priority - Support roles
        if any(keyword in title_lower for keyword in ['assistant', 'associate', 'representative']):
            return 40
            
        # Default
        return 30

    def _gpt_extract_staff(self, content: str, url: str) -> list:
        """🧠 ENHANCED: GPT-4o extracts staff from content with better validation"""
        
        if len(content) < 2000:
            return []
        
        domain = urlparse(url).netloc.replace('www.', '')
        
        # Extract staff-related sections first
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
{content[:100000]}"""

        try:
            from openai import OpenAI
            client = OpenAI(api_key=self.openai_key)
            
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=1500,
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
                
                # Validate staff
                validated_staff = []
                for staff in staff_list:
                    name = staff.get('name', '')
                    title = staff.get('title', '')
                    
                    if self._validate_staff_name(name, domain):
                        validated_staff.append(staff)
                        print(f"✅ Valid staff: {name} - {title}")
                    else:
                        print(f"❌ Invalid staff: {name} - {title}")
                
                return validated_staff[:10]
                
        except Exception as e:
            print(f"    ⚠️ GPT extraction failed: {e}")
        
        return []
    
    def _extract_staff_sections(self, content: str) -> str:
        """🔍 Extract staff-related sections from content"""
        staff_keywords = [
            'about us', 'team', 'staff', 'our people', 'leadership',
            'meet the team', 'our team', 'employees', 'directors'
        ]
        
        lines = content.split('\n')
        staff_lines = []
        in_staff_section = False
        
        for line in lines:
            line_lower = line.lower().strip()
            
            if any(keyword in line_lower for keyword in staff_keywords):
                in_staff_section = True
                staff_lines.append(line)
                continue
            
            if in_staff_section:
                staff_lines.append(line)
                
                if len(line.strip()) == 0 and len(staff_lines) > 20:
                    break
        
        return '\n'.join(staff_lines) if staff_lines else content
    
    def _validate_staff_name(self, name: str, domain: str) -> bool:
        """✅ Validate if name looks like a real person"""
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