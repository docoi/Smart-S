"""
🌐 Website Scraper Module
========================
Handles website scraping, staff extraction, and LinkedIn URL discovery
"""

import json
from urllib.parse import urlparse, urlunparse
from account_manager import get_working_apify_client_part1


class WebsiteScraper:
    """🌐 Website scraping with GPT-4o analysis"""
    
    def __init__(self, openai_key):
        self.openai_key = openai_key
    
    def scrape_website_for_staff_and_linkedin(self, website_url: str) -> tuple:
        """📋 Main method: Scrape website for staff and LinkedIn URL using account rotation"""
        
        print(f"🗺️ Phase 1a: Website mapping...")
        
        # Normalize URL
        normalized_url = self._normalize_www(website_url)
        
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
        
        print(f"🔍 Phase 1c: Content analysis...")
        
        # Extract staff and LinkedIn from selected URLs using account rotation
        staff_list, social_data = self._extract_staff_and_social(selected_urls)
        
        # 🔧 CRITICAL: Properly format LinkedIn URL for Part 2
        linkedin_url = (social_data.get('company_linkedin') or 
                       social_data.get('linkedin') or 
                       social_data.get('LinkedIn') or "")
        
        # 🔧 Clean and format LinkedIn URL for Actor 2 compatibility
        if linkedin_url:
            linkedin_url = self._format_linkedin_url_for_part2(linkedin_url)
        
        print(f"✅ Website scraping complete:")
        print(f"   👥 Staff found: {len(staff_list)}")
        if linkedin_url:
            print(f"   🔗 LinkedIn URL: {linkedin_url}")
            print(f"   🔧 Formatted for Part 2: ✅")
        else:
            print(f"   ❌ No LinkedIn URL found")
        
        return staff_list, linkedin_url

    def _format_linkedin_url_for_part2(self, linkedin_url: str) -> str:
        """🔧 CRITICAL: Format LinkedIn URL for Part 2 Actor compatibility"""
        
        try:
            print(f"🔧 Original LinkedIn URL: {linkedin_url}")
            
            # Remove tracking parameters and fragments
            if '?' in linkedin_url:
                linkedin_url = linkedin_url.split('?')[0]
            if '#' in linkedin_url:
                linkedin_url = linkedin_url.split('#')[0]
            
            # Ensure proper protocol
            if not linkedin_url.startswith('http'):
                linkedin_url = 'https://' + linkedin_url
            
            # Standardize domain format
            linkedin_url = linkedin_url.replace('uk.linkedin.com', 'www.linkedin.com')
            linkedin_url = linkedin_url.replace('//linkedin.com', '//www.linkedin.com')
            
            # Ensure www prefix
            if '//linkedin.com' in linkedin_url:
                linkedin_url = linkedin_url.replace('//linkedin.com', '//www.linkedin.com')
            
            # Ensure /company/ format and clean ending
            if '/company/' in linkedin_url:
                linkedin_url = linkedin_url.rstrip('/')
                
                print(f"🔧 Formatted LinkedIn URL: {linkedin_url}")
                print(f"🔧 Ready for Part 2 Actor: ✅")
                return linkedin_url
            
            print(f"⚠️ LinkedIn URL format not recognized: {linkedin_url}")
            return linkedin_url
            
        except Exception as e:
            print(f"⚠️ Error formatting LinkedIn URL: {e}")
            return linkedin_url

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
        """🧠 GPT-4o analyzes URLs to select best INTERNAL pages for staff extraction"""
        
        all_links = website_map.get('allLinks', [])
        
        # 🔧 CRITICAL: Filter out social media for CONTENT ANALYSIS only
        # (We still collect social media during content extraction for LinkedIn URL)
        internal_links = []
        domain_name = urlparse(domain).netloc.replace('www.', '')
        
        for link in all_links:
            url = link.get('url', '')
            href = link.get('href', '')
            
            # Skip if no URL
            if not url:
                continue
            
            # 🔧 Filter out social media URLs from content analysis selection
            social_sites = ['linkedin.com', 'facebook.com', 'instagram.com', 'twitter.com', 'youtube.com']
            if any(social in url.lower() for social in social_sites):
                continue
            
            # 🔧 Keep only same-domain links for content analysis
            try:
                link_domain = urlparse(url).netloc.replace('www.', '')
                if link_domain and link_domain != domain_name:
                    continue
            except:
                continue
            
            internal_links.append(link)
        
        # 🔧 Add common team pages that might be missing from sitemap
        common_team_pages = [
            f"{domain}/about",
            f"{domain}/team", 
            f"{domain}/contact",
            f"{domain}/about-us",
            f"{domain}#the-team-behind-your-team"  # For Crewsaders carousel
        ]
        
        for page_url in common_team_pages:
            internal_links.append({
                'url': page_url,
                'text': 'Team/About Page',
                'href': page_url.replace(domain, '')
            })
        
        # Create summary for GPT (internal pages only)
        link_summary = []
        for link in internal_links[:50]:
            url = link.get('url', '')
            text = link.get('text', '')
            href = link.get('href', '')
            link_summary.append(f"{text} → {href}")
        
        prompt = f"""Analyze this website and select the 3 BEST INTERNAL pages for finding staff information.

DOMAIN: {domain}
INTERNAL WEBSITE PAGES:
{chr(10).join(link_summary)}

Select pages most likely to contain staff/team information from {domain} only.
Focus on: about, team, staff, people, contact, leadership pages.

Return as JSON:
{{
  "selected_urls": [
    "internal_url_1",
    "internal_url_2", 
    "internal_url_3"
  ]
}}"""

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
                
                print(f"🎯 GPT-4o selected {len(urls)} internal URLs:")
                for i, url in enumerate(urls, 1):
                    print(f"   {i}. {url}")
                
                return urls
                
        except Exception as e:
            print(f"⚠️ GPT analysis failed: {e}")
        
        # 🔧 ENHANCED FALLBACK: Use common team pages
        print("🔄 Using fallback internal page selection...")
        selected = []
        
        # First try common team pages
        for page_url in common_team_pages:
            selected.append(page_url)
            if len(selected) >= 3:
                break
        
        print(f"🔄 Fallback selected {len(selected)} internal URLs:")
        for i, url in enumerate(selected, 1):
            print(f"   {i}. {url}")
        
        return selected

    def _extract_staff_and_social(self, urls: list) -> tuple:
        """🔍 Extract staff and social media from selected URLs"""
        
        all_staff = []
        all_social = {}
        
        for i, url in enumerate(urls, 1):
            print(f"  📄 Processing {i}/{len(urls)}: {url}")
            
            try:
                staff, social = self._analyze_single_url(url)
                all_staff.extend(staff)
                all_social.update(social)
                
                if staff:
                    print(f"    ✅ Found {len(staff)} staff members")
                
            except Exception as e:
                print(f"    ⚠️ Error: {e}")
                
        return all_staff, all_social

    def _analyze_single_url(self, url: str) -> tuple:
        """🔍 Analyze single URL for staff and social media using account rotation"""
        
        # Clean JavaScript function
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
                    
                    return staff, social
            
            return [], {}
            
        except Exception as e:
            print(f"    ❌ Content analysis failed: {e}")
            return [], {}

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