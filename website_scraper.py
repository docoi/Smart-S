"""
🌐 Website Scraper Module - COMPLETE FIX
========================================
Fixed LinkedIn URL corruption issue
"""

import json
import re
from urllib.parse import urlparse, urlunparse
from account_manager import get_working_apify_client_part1


class WebsiteScraper:
    """🌐 Website scraping with FIXED LinkedIn URL handling"""
    
    def __init__(self, openai_key):
        self.openai_key = openai_key
    
    def scrape_website_for_staff_and_linkedin(self, website_url: str) -> tuple:
        """📋 Main method: Scrape website for staff and LinkedIn URL using APIFY_TOKEN_1"""
        
        print(f"🗺️ Phase 1a: Website mapping...")
        
        # Normalize URL
        normalized_url = self._normalize_www(website_url)
        
        # Map website to find URLs using APIFY_TOKEN_1
        website_map = self._stealth_website_mapping(normalized_url)
        
        if not website_map:
            print("❌ Website mapping failed")
            return [], ""
        
        print(f"🧠 Phase 1b: GPT-4o URL analysis...")
        
        # Extract LinkedIn URL FIRST, before GPT analysis
        linkedin_url = self._extract_linkedin_url_from_map(website_map, normalized_url)
        
        # Analyze with GPT-4o to select best URLs (EXCLUDING LinkedIn)
        selected_urls = self._gpt_analyze_urls(website_map, normalized_url)
        
        if not selected_urls:
            print("❌ No URLs selected for analysis")
            return [], linkedin_url  # Still return LinkedIn URL if found
        
        print(f"🔍 Phase 1c: Content analysis...")
        
        # Extract staff from selected URLs using APIFY_TOKEN_1
        staff_list, social_data = self._extract_staff_and_social(selected_urls)
        
        # If no LinkedIn URL found yet, try from social data
        if not linkedin_url:
            linkedin_url = self._get_clean_linkedin_url(social_data)
        
        print(f"✅ Website scraping complete:")
        print(f"   👥 Staff found: {len(staff_list)}")
        print(f"   🔗 LinkedIn URL: {linkedin_url}" if linkedin_url else "   ❌ No LinkedIn URL found")
        
        return staff_list, linkedin_url

    def _extract_linkedin_url_from_map(self, website_map: dict, target_domain: str) -> str:
        """🔧 FIXED: Extract LinkedIn URL directly from website map"""
        
        all_links = website_map.get('allLinks', [])
        target_company = urlparse(target_domain).netloc.replace('www.', '').replace('.co.uk', '').replace('.com', '')
        
        print(f"🔍 Searching for LinkedIn URL for company: {target_company}")
        
        linkedin_candidates = []
        
        for link in all_links:
            url = link.get('url', '').lower()
            if 'linkedin.com' in url:
                linkedin_candidates.append(link.get('url', ''))
        
        if not linkedin_candidates:
            print("   ⚠️ No LinkedIn URLs found in website map")
            return ""
        
        print(f"   🔍 Found {len(linkedin_candidates)} LinkedIn URL candidates:")
        for i, candidate in enumerate(linkedin_candidates, 1):
            print(f"      {i}. {candidate}")
        
        # Clean and validate each candidate
        for candidate in linkedin_candidates:
            cleaned_url = self._clean_linkedin_url(candidate, target_company)
            if cleaned_url:
                print(f"   ✅ Selected LinkedIn URL: {cleaned_url}")
                return cleaned_url
        
        print("   ❌ No valid LinkedIn URL found after cleaning")
        return ""

    def _clean_linkedin_url(self, raw_url: str, target_company: str) -> str:
        """🧽 FIXED: Clean LinkedIn URL to proper /company/ format"""
        
        if not raw_url or 'linkedin.com' not in raw_url.lower():
            return ""
        
        try:
            # Remove tracking parameters and clean URL
            url = raw_url.split('?')[0].split('#')[0]
            
            # Ensure proper format
            if not url.startswith('http'):
                url = 'https://' + url
            
            # Parse URL
            parsed = urlparse(url)
            
            # Clean netloc to www.linkedin.com
            netloc = 'www.linkedin.com'
            
            # Extract company identifier
            path_parts = [part for part in parsed.path.split('/') if part]
            
            if len(path_parts) >= 2 and path_parts[0] == 'company':
                # Already in correct format: /company/company-name
                company_name = path_parts[1]
                clean_path = f"/company/{company_name}"
            elif len(path_parts) >= 1:
                # Try to extract company name from various formats
                if 'company' in parsed.path:
                    # Find company name after 'company'
                    for i, part in enumerate(path_parts):
                        if part == 'company' and i + 1 < len(path_parts):
                            company_name = path_parts[i + 1]
                            clean_path = f"/company/{company_name}"
                            break
                    else:
                        return ""
                else:
                    # Try to match with target company
                    for part in path_parts:
                        if target_company.lower() in part.lower() or part.lower() in target_company.lower():
                            clean_path = f"/company/{part}"
                            break
                    else:
                        # Use first path part as company name
                        company_name = path_parts[0]
                        clean_path = f"/company/{company_name}"
            else:
                return ""
            
            # Reconstruct clean URL
            clean_url = urlunparse(('https', netloc, clean_path, '', '', ''))
            
            print(f"      🧽 Cleaned: {raw_url} → {clean_url}")
            return clean_url
            
        except Exception as e:
            print(f"      ⚠️ Error cleaning LinkedIn URL {raw_url}: {e}")
            return ""

    def _stealth_website_mapping(self, url: str) -> dict:
        """🗺️ Stealth website mapping using APIFY_TOKEN_1"""
        
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
            # Get client using APIFY_TOKEN_1
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
        """🧠 FIXED: GPT-4o analyzes URLs but EXCLUDES LinkedIn URLs"""
        
        all_links = website_map.get('allLinks', [])
        
        # 🔧 FIXED: Filter OUT LinkedIn URLs from GPT analysis
        filtered_links = []
        for link in all_links[:50]:
            url = link.get('url', '').lower()
            if 'linkedin.com' not in url:  # Skip LinkedIn URLs
                filtered_links.append(link)
        
        # Create link summary for GPT (WITHOUT LinkedIn URLs)
        link_summary = []
        for link in filtered_links:
            url = link.get('url', '')
            text = link.get('text', '')
            href = link.get('href', '')
            link_summary.append(f"{text} → {href}")
        
        prompt = f"""Analyze this website and select the 3 BEST URLs for finding staff information.

DOMAIN: {domain}
AVAILABLE LINKS (LinkedIn URLs excluded):
{chr(10).join(link_summary)}

Select URLs most likely to contain staff/team information. Return as JSON:
{{
  "selected_urls": [
    "full_url_1",
    "full_url_2", 
    "full_url_3"
  ]
}}

IMPORTANT: Focus ONLY on: about, team, staff, people, contact, leadership, who pages from the SAME DOMAIN."""

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
                
                # 🔧 FIXED: Validate URLs are from same domain
                validated_urls = []
                target_domain = urlparse(domain).netloc.replace('www.', '')
                
                for url in urls:
                    try:
                        url_domain = urlparse(url).netloc.replace('www.', '')
                        if url_domain == target_domain:
                            validated_urls.append(url)
                        else:
                            print(f"   ⚠️ Skipping external URL: {url}")
                    except:
                        print(f"   ⚠️ Invalid URL format: {url}")
                
                print(f"🎯 GPT-4o selected {len(validated_urls)} valid URLs:")
                for i, url in enumerate(validated_urls, 1):
                    print(f"   {i}. {url}")
                
                return validated_urls
                
        except Exception as e:
            print(f"⚠️ GPT analysis failed: {e}")
        
        # Fallback: keyword-based selection (exclude LinkedIn)
        staff_keywords = ["/team", "/about", "/staff", "/people", "/contact", "/who"]
        selected = []
        
        for link in all_links:
            url = link.get('url', '').lower()
            if ('linkedin.com' not in url and 
                any(keyword in url for keyword in staff_keywords)):
                selected.append(link.get('url', ''))
                if len(selected) >= 3:
                    break
        
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
        """🔍 Analyze single URL for staff and social media using APIFY_TOKEN_1"""
        
        # Enhanced JavaScript function for better social media extraction
        page_function = """
        async function pageFunction(context) {
            const { request, log, jQuery } = context;
            const $ = jQuery;
            
            await context.waitFor(8000);
            
            try {
                const bodyText = $('body').text() || '';
                const socialMedia = {};
                
                // Enhanced social media extraction
                $('a[href]').each(function() {
                    const href = $(this).attr('href');
                    if (!href) return;
                    
                    const lowerHref = href.toLowerCase();
                    
                    // LinkedIn extraction with multiple patterns
                    if (lowerHref.includes('linkedin.com') && !socialMedia.linkedin) {
                        // Clean LinkedIn URL - prefer /company/ format
                        if (lowerHref.includes('/company/')) {
                            socialMedia.linkedin = href;
                        } else if (!socialMedia.linkedin) {
                            socialMedia.linkedin = href;
                        }
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
            # Get client using APIFY_TOKEN_1
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

    def _get_clean_linkedin_url(self, social_data: dict) -> str:
        """🔧 Get clean LinkedIn URL from social data as fallback"""
        
        linkedin_candidates = []
        
        # Collect from social media data
        for key, value in social_data.items():
            if isinstance(value, str) and 'linkedin.com' in value.lower():
                linkedin_candidates.append(value)
        
        if not linkedin_candidates:
            return ""
        
        # Clean and validate each candidate
        for candidate in linkedin_candidates:
            cleaned_url = self._clean_linkedin_url(candidate, "")
            if cleaned_url:
                return cleaned_url
        
        return ""

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