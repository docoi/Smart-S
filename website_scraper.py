"""
🌐 Website Scraper Module - FIXED INDENTATION
==============================================
Clean version with proper indentation and method structure
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
        
        # Find LinkedIn URL
        linkedin_url = (social_data.get('company_linkedin') or 
                       social_data.get('linkedin') or 
                       social_data.get('LinkedIn') or "")
        
        print(f"✅ Website scraping complete:")
        print(f"   👥 Staff found: {len(staff_list)}")
        print(f"   🔗 LinkedIn URL: {linkedin_url[:50]}..." if linkedin_url else "   ❌ No LinkedIn URL found")
        
        return staff_list, linkedin_url

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
        """🧠 GPT-4o analyzes URLs to select best ones for staff extraction - FIXED SOCIAL MEDIA FILTERING"""
        
        all_links = website_map.get('allLinks', [])
        
        # ✅ FILTER OUT social media and external links - KEEP ONLY INTERNAL PAGES
        internal_links = []
        parsed_domain = urlparse(domain).netloc.replace('www.', '')
        
        for link in all_links:
            url = link.get('url', '')
            
            # Skip empty URLs
            if not url:
                continue
                
            # Parse the URL to check domain
            try:
                parsed_url = urlparse(url)
                link_domain = parsed_url.netloc.replace('www.', '')
                
                # ❌ SKIP external domains (social media, etc.)
                if link_domain and link_domain != parsed_domain:
                    continue
                    
                # ❌ SKIP obvious social media URLs
                social_keywords = ['linkedin', 'facebook', 'twitter', 'instagram', 'youtube', 'tiktok']
                if any(social in url.lower() for social in social_keywords):
                    continue
                    
                # ❌ SKIP file downloads and non-page URLs
                file_extensions = ['.pdf', '.doc', '.docx', '.zip', '.jpg', '.png', '.mp4']
                if any(ext in url.lower() for ext in file_extensions):
                    continue
                
                # ✅ KEEP internal website pages only
                internal_links.append(link)
                
            except Exception:
                # If URL parsing fails, skip it
                continue
        
        # ✅ ADD potential team pages that might be missing from sitemap
        potential_team_urls = [
            f"{domain}/about",
            f"{domain}/team", 
            f"{domain}/our-team",
            f"{domain}/staff",
            f"{domain}/people",
            f"{domain}/contact",
            f"{domain}/about-us",
            f"{domain}/leadership",
            f"{domain}/management",
            f"{domain}#team",
            f"{domain}#the-team",
            f"{domain}#our-team",
            f"{domain}#the-team-behind-your-team"  # ✅ For Crewsaders specifically
        ]
        
        # Add potential URLs to the list
        for potential_url in potential_team_urls:
            internal_links.append({
                'url': potential_url,
                'text': 'Team Page',
                'href': potential_url.replace(domain, '')
            })
        
        # Create link summary for GPT (internal links only)
        link_summary = []
        for link in internal_links[:50]:  # Top 50 to avoid token limits
            url = link.get('url', '')
            text = link.get('text', '')
            href = link.get('href', '')
            link_summary.append(f"{text} → {href}")
        
        prompt = f"""Analyze this website and select the 3-4 BEST INTERNAL URLs for finding staff/team information.

DOMAIN: {domain}
INTERNAL WEBSITE LINKS ONLY (NO SOCIAL MEDIA):
{chr(10).join(link_summary)}

RULES:
1. SELECT ONLY internal pages from {domain}
2. PRIORITIZE: team, staff, about, people, contact, leadership pages
3. LOOK FOR: #team, #our-team, #the-team-behind-your-team (URL fragments)
4. EXCLUDE: Social media links (LinkedIn, Facebook, Instagram, etc.)
5. FOCUS: Pages most likely to contain employee/staff information

Return as JSON:
{{
  "selected_urls": [
    "internal_url_1",
    "internal_url_2", 
    "internal_url_3",
    "internal_url_4"
  ]
}}

SELECT PAGES FROM THE WEBSITE ITSELF, NOT EXTERNAL SOCIAL MEDIA."""

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
                
                # ✅ FINAL FILTER: Ensure all URLs are internal to the domain
                filtered_urls = []
                for url in urls:
                    if url and domain.replace('https://', '').replace('www.', '') in url:
                        filtered_urls.append(url)
                
                print(f"🎯 GPT-4o selected {len(filtered_urls)} valid internal URLs:")
                for i, url in enumerate(filtered_urls, 1):
                    print(f"   {i}. {url}")
                
                return filtered_urls[:4]  # Return top 4
                
        except Exception as e:
            print(f"⚠️ GPT analysis failed: {e}")
        
        # ✅ ENHANCED FALLBACK: Smart internal URL selection
        print("🔄 Using enhanced fallback URL selection...")
        staff_keywords = ["/team", "/about", "/staff", "/people", "/contact", "/leadership"]
        selected = []
        
        # First, try to find exact keyword matches
        for link in internal_links:
            url = link.get('url', '').lower()
            if any(keyword in url for keyword in staff_keywords):
                if link.get('url') not in selected:
                    selected.append(link.get('url'))
                if len(selected) >= 3:
                    break
        
        # If not enough found, add the main page and common team pages
        if len(selected) < 3:
            common_pages = [
                f"{domain}/about",
                f"{domain}/team",
                f"{domain}#the-team-behind-your-team"  # For Crewsaders
            ]
            for page in common_pages:
                if page not in selected:
                    selected.append(page)
                if len(selected) >= 3:
                    break
        
        print(f"🔄 Fallback selected {len(selected)} URLs:")
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
        
        # Higher minimum content threshold
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
                
                # Better staff validation
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
        """🔥 Extract staff-related sections from content"""
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
        """🔥 Validate if name looks like a real person"""
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