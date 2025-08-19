"""
🌐 Website Scraper Module
========================
Handles website scraping, staff extraction, and LinkedIn URL discovery
Enhanced with JavaScript handling and GPT-4o direct web access fallback
"""

import json
import requests
from urllib.parse import urlparse, urlunparse
from account_manager import get_working_apify_client_part1


class WebsiteScraper:
    """🌐 Website scraping with GPT-4o analysis and JavaScript/roadblock handling"""
    
    def __init__(self, openai_key):
        self.openai_key = openai_key
    
    def scrape_website_for_staff_and_linkedin(self, website_url: str) -> tuple:
        """📋 Main method: Scrape website for staff and LinkedIn URL with fallback strategies"""
        
        print(f"🗺️ Phase 1a: Website mapping...")
        
        # Normalize URL
        normalized_url = self._normalize_www(website_url)
        
        # Try Actor 1 first
        website_map = self._stealth_website_mapping(normalized_url)
        
        # Check if Actor 1 hit a roadblock
        if self._is_actor1_roadblock(website_map):
            print("🚧 Actor 1 roadblock detected - switching to GPT-4o direct web access")
            return self._gpt4o_direct_web_access_workflow(normalized_url)
        
        if not website_map:
            print("❌ Website mapping failed completely")
            return [], ""
        
        print(f"🧠 Phase 1b: GPT-4o URL analysis...")
        
        # Analyze with GPT-4o to select best URLs
        selected_urls = self._gpt_analyze_urls_enhanced(website_map, normalized_url)
        
        if not selected_urls:
            print("❌ No URLs selected for analysis")
            return [], ""
        
        print(f"🔍 Phase 1c: Content analysis...")
        
        # Extract staff and LinkedIn from selected URLs
        staff_list, social_data = self._extract_staff_and_social(selected_urls)
        
        # Find LinkedIn URL
        linkedin_url = (social_data.get('company_linkedin') or 
                       social_data.get('linkedin') or 
                       social_data.get('LinkedIn') or 
                       website_map.get('socialLinks', {}).get('linkedin') or "")
        
        print(f"✅ Website scraping complete:")
        print(f"   👥 Staff found: {len(staff_list)}")
        print(f"   🔗 LinkedIn URL: {linkedin_url[:50]}..." if linkedin_url else "   ❌ No LinkedIn URL found")
        
        return staff_list, linkedin_url

    def _is_actor1_roadblock(self, website_map: dict) -> bool:
        """🚧 Detect if Actor 1 hit a roadblock (JavaScript, empty content, etc.)"""
        
        if not website_map:
            return True
        
        stats = website_map.get('scrapingStats', {})
        page_info = website_map.get('pageInfo', {})
        
        # Roadblock indicators
        total_links = stats.get('totalLinks', 0)
        has_content = page_info.get('hasContent', False)
        body_length = page_info.get('bodyLength', 0)
        has_error = stats.get('error', False)
        
        roadblock_detected = (
            total_links < 3 or          # Very few links found
            not has_content or          # No meaningful content
            body_length < 200 or        # Very short content
            has_error                   # Scraping error occurred
        )
        
        if roadblock_detected:
            print(f"🚧 Roadblock detected: links={total_links}, content={has_content}, length={body_length}, error={has_error}")
        
        return roadblock_detected

    def _gpt4o_direct_web_access_workflow(self, website_url: str) -> tuple:
        """🤖 GPT-4o direct web access workflow - bypasses all technical barriers"""
        
        print("🤖 GPT-4O DIRECT WEB ACCESS WORKFLOW")
        print("🔧 Bypassing JavaScript, dynamic content, and technical barriers")
        print("=" * 70)
        
        try:
            from openai import OpenAI
            client = OpenAI(api_key=self.openai_key)
            
            domain = urlparse(website_url).netloc.replace('www.', '')
            
            # Phase 1: GPT-4o website analysis and URL discovery
            analysis_prompt = f"""You are a professional web researcher. Analyze the website {website_url} to find staff/team information and LinkedIn company URL.

TASK 1: WEBSITE ANALYSIS
- Visit and analyze {website_url}
- Identify ALL pages that might contain staff/team information
- Look for: /about, /team, /staff, /people, /leadership, /management, /contact
- Find the company's LinkedIn profile URL
- Note any JavaScript barriers or dynamic content issues

TASK 2: CONTENT STRATEGY
- Determine the best 3-5 URLs for staff extraction
- Identify common page structures and navigation
- Locate social media links, especially LinkedIn

Return detailed analysis in this format:
WEBSITE_ANALYSIS:
- Main domain: {domain}
- Site structure: [description]
- Navigation type: [menu/links structure]
- JavaScript dependency: [yes/no and impact]

RECOMMENDED_URLS:
1. [full_url] - [reason for selection]
2. [full_url] - [reason for selection]
3. [full_url] - [reason for selection]

LINKEDIN_URL: [company LinkedIn URL if found]

BARRIERS_DETECTED: [any technical issues encountered]"""

            print("🔍 Phase 1: GPT-4o analyzing website structure...")
            
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": analysis_prompt}],
                max_tokens=1500,
                temperature=0.1
            )
            
            analysis_result = response.choices[0].message.content.strip()
            print(f"🧠 GPT-4o Website Analysis:")
            print(f"{analysis_result}")
            
            # Extract URLs and LinkedIn from GPT-4o analysis
            recommended_urls = self._extract_urls_from_gpt_analysis(analysis_result, website_url)
            linkedin_url = self._extract_linkedin_from_gpt_analysis(analysis_result)
            
            if not recommended_urls:
                print("❌ GPT-4o could not identify staff URLs")
                return [], linkedin_url or ""
            
            print(f"🎯 GPT-4o recommended {len(recommended_urls)} URLs for staff extraction")
            
            # Phase 2: GPT-4o direct content extraction
            all_staff = []
            
            for i, url in enumerate(recommended_urls, 1):
                print(f"\n🔍 Phase 2.{i}: GPT-4o extracting staff from {url}")
                
                staff_from_page = self._gpt4o_extract_staff_from_url(url, domain)
                
                if staff_from_page:
                    all_staff.extend(staff_from_page)
                    print(f"   ✅ Found {len(staff_from_page)} staff members")
                else:
                    print(f"   ❌ No staff found on this page")
            
            # Remove duplicates
            unique_staff = []
            seen_names = set()
            
            for staff in all_staff:
                name = staff.get('name', '').strip().lower()
                if name and name not in seen_names:
                    seen_names.add(name)
                    unique_staff.append(staff)
            
            print(f"\n🎉 GPT-4o Direct Access Results:")
            print(f"   👥 Total staff found: {len(unique_staff)}")
            print(f"   🔗 LinkedIn URL: {linkedin_url or 'Not found'}")
            print(f"   🚧 Barriers bypassed: JavaScript, dynamic content, technical restrictions")
            
            return unique_staff, linkedin_url or ""
            
        except Exception as e:
            print(f"❌ GPT-4o direct web access failed: {e}")
            return [], ""

    def _extract_urls_from_gpt_analysis(self, analysis_text: str, base_url: str) -> list:
        """📝 Extract recommended URLs from GPT-4o analysis"""
        
        urls = []
        lines = analysis_text.split('\n')
        
        for line in lines:
            line = line.strip()
            
            # Look for numbered URLs in recommendations
            if any(pattern in line.lower() for pattern in ['http', 'www.', base_url.split('//')[1]]):
                # Extract URL from the line
                words = line.split()
                for word in words:
                    if any(url_part in word for url_part in ['http', 'www.', base_url.split('//')[1]]):
                        # Clean up the URL
                        url = word.strip('[]().,"-')
                        if url.startswith('http') or url.startswith('www'):
                            if not url.startswith('http'):
                                url = 'https://' + url
                            urls.append(url)
                            break
        
        # Fallback: generate common URLs if none found
        if not urls:
            domain = urlparse(base_url).netloc
            scheme = urlparse(base_url).scheme
            
            common_paths = ['/about', '/team', '/staff', '/people', '/contact', '/leadership']
            for path in common_paths:
                urls.append(f"{scheme}://{domain}{path}")
        
        return urls[:5]  # Return max 5 URLs

    def _extract_linkedin_from_gpt_analysis(self, analysis_text: str) -> str:
        """🔗 Extract LinkedIn URL from GPT-4o analysis"""
        
        lines = analysis_text.split('\n')
        
        for line in lines:
            if 'linkedin' in line.lower() and 'http' in line.lower():
                words = line.split()
                for word in words:
                    if 'linkedin.com' in word.lower():
                        url = word.strip('[]().,"-')
                        if url.startswith('http'):
                            return url
                        elif url.startswith('linkedin.com') or url.startswith('www.linkedin.com'):
                            return 'https://' + url.replace('www.', '')
        
        return ""

    def _gpt4o_extract_staff_from_url(self, url: str, domain: str) -> list:
        """🤖 GPT-4o extracts staff directly from URL - bypasses all technical barriers"""
        
        try:
            from openai import OpenAI
            client = OpenAI(api_key=self.openai_key)
            
            extraction_prompt = f"""You are a professional data extraction specialist. Extract staff/employee information from the webpage: {url}

EXTRACTION RULES:
1. Find ONLY current employees of {domain}
2. Extract: Full name + Job title/position
3. EXCLUDE: Clients, testimonials, external people, board members not employed
4. EXCLUDE: Company names mistaken as people
5. Require: Minimum first name + last name

TECHNICAL REQUIREMENTS:
- Bypass any JavaScript loading barriers
- Navigate through dynamic content if needed
- Access full page content including lazy-loaded sections
- Handle any technical restrictions or access barriers

OUTPUT FORMAT - Return as JSON array:
[
  {{
    "name": "Full Name",
    "title": "Job Title/Position"
  }}
]

If no staff found, return: []

IMPORTANT: Only return the JSON array, no other text."""

            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": extraction_prompt}],
                max_tokens=2000,
                temperature=0.1
            )
            
            result_text = response.choices[0].message.content.strip()
            
            # Parse JSON response
            try:
                # Find JSON array in the response
                json_start = result_text.find('[')
                json_end = result_text.rfind(']') + 1
                
                if json_start >= 0 and json_end > json_start:
                    json_text = result_text[json_start:json_end]
                    staff_list = json.loads(json_text)
                    
                    # Validate staff entries
                    validated_staff = []
                    for staff in staff_list:
                        if isinstance(staff, dict) and staff.get('name') and staff.get('title'):
                            name = staff['name'].strip()
                            title = staff['title'].strip()
                            
                            if self._validate_staff_name(name, domain):
                                validated_staff.append({
                                    'name': name,
                                    'title': title
                                })
                    
                    return validated_staff
                
            except json.JSONDecodeError as e:
                print(f"   ⚠️ JSON parsing error: {e}")
                return []
            
            return []
            
        except Exception as e:
            print(f"   ❌ GPT-4o extraction error: {e}")
            return []

    def _stealth_website_mapping(self, url: str) -> dict:
        """🗺️ ENHANCED: Stealth website mapping with JavaScript handling"""
        
        # Enhanced JavaScript function with better detection
        mapping_function = """
async function pageFunction(context) {
    const { request, log, jQuery } = context;
    const $ = jQuery;
    
    // Enhanced wait for JavaScript-heavy sites
    await context.waitFor(15000);
    
    try {
        // Method 1: Standard link extraction
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
        
        // Method 2: Navigation and menu links
        const navigationLinks = [];
        $('nav a, .navigation a, .menu a, header a').each(function() {
            const href = $(this).attr('href');
            const text = $(this).text().trim();
            if (href && text) {
                navigationLinks.push({ href, text });
            }
        });
        
        // Method 3: Social media detection
        const socialLinks = {};
        $('a[href*="linkedin"]').each(function() {
            if (!socialLinks.linkedin) socialLinks.linkedin = $(this).attr('href');
        });
        $('a[href*="facebook"]').each(function() {
            if (!socialLinks.facebook) socialLinks.facebook = $(this).attr('href');
        });
        $('a[href*="twitter"]').each(function() {
            if (!socialLinks.twitter) socialLinks.twitter = $(this).attr('href');
        });
        $('a[href*="instagram"]').each(function() {
            if (!socialLinks.instagram) socialLinks.instagram = $(this).attr('href');
        });
        
        // Method 4: Content analysis
        const bodyText = $('body').text() || '';
        const pageTitle = $('title').text() || '';
        const metaDescription = $('meta[name="description"]').attr('content') || '';
        
        return {
            url: request.url,
            websiteMap: {
                allLinks: allLinks,
                navigationLinks: navigationLinks,
                socialLinks: socialLinks,
                pageInfo: {
                    title: pageTitle,
                    description: metaDescription,
                    bodyLength: bodyText.length,
                    hasContent: bodyText.length > 100
                },
                domain: request.url.split('/')[2],
                scrapingStats: {
                    totalLinks: allLinks.length,
                    navigationLinks: navigationLinks.length,
                    socialLinksFound: Object.keys(socialLinks).length,
                    error: false
                }
            }
        };
        
    } catch (error) {
        return {
            url: request.url,
            websiteMap: { 
                allLinks: [], 
                domain: request.url.split('/')[2],
                scrapingStats: { totalLinks: 0, error: true, errorMessage: error.message }
            }
        };
    }
}
"""
        
        payload = {
            "startUrls": [{"url": url}],
            "maxPagesPerRun": 1,
            "pageFunction": mapping_function,
            "proxyConfiguration": {"useApifyProxy": True},
            "waitUntil": ["domcontentloaded", "networkidle2"],
            "pageLoadTimeoutSecs": 60,
            "maxRequestRetries": 3
        }
        
        try:
            # Get client with credit-based account rotation for Part 1
            client = get_working_apify_client_part1()
            
            run = client.actor("apify~web-scraper").call(run_input=payload)
            
            if run:
                items = client.dataset(run["defaultDatasetId"]).list_items().items
                if items:
                    website_map = items[0].get('websiteMap', {})
                    
                    # Add fallback URLs if minimal content
                    if website_map.get('scrapingStats', {}).get('totalLinks', 0) < 5:
                        website_map = self._add_fallback_urls(website_map, url)
                    
                    return website_map
            
            return {}
            
        except Exception as e:
            print(f"❌ Enhanced website mapping failed: {e}")
            return {}

    def _add_fallback_urls(self, website_map: dict, url: str) -> dict:
        """🔄 Add fallback URLs for minimal content sites"""
        
        domain = urlparse(url).netloc
        scheme = urlparse(url).scheme
        
        fallback_urls = [
            f"{scheme}://{domain}/about",
            f"{scheme}://{domain}/team", 
            f"{scheme}://{domain}/staff",
            f"{scheme}://{domain}/people",
            f"{scheme}://{domain}/contact",
            f"{scheme}://{domain}/leadership"
        ]
        
        fallback_links = []
        for fallback_url in fallback_urls:
            fallback_links.append({
                'url': fallback_url,
                'text': fallback_url.split('/')[-1].replace('-', ' ').title(),
                'href': '/' + fallback_url.split('/')[-1],
                'source': 'fallback'
            })
        
        website_map['allLinks'].extend(fallback_links)
        website_map['fallbackAdded'] = len(fallback_links)
        
        print(f"🔄 Added {len(fallback_links)} fallback URLs")
        return website_map

    def _gpt_analyze_urls_enhanced(self, website_map: dict, domain: str) -> list:
        """🧠 ENHANCED: GPT-4o analyzes URLs with better context and fallback handling"""
        
        all_links = website_map.get('allLinks', [])
        social_links = website_map.get('socialLinks', {})
        page_info = website_map.get('pageInfo', {})
        
        # Filter internal links for analysis
        internal_links = []
        for link in all_links[:30]:
            url = link.get('url', '').lower()
            
            # Skip social media for content analysis
            if any(social in url for social in ['linkedin.com', 'facebook.com', 'twitter.com', 'instagram.com']):
                continue
                
            internal_links.append(f"{link.get('text', '')} → {link.get('href', '')}")
        
        prompt = f"""Analyze website {domain} for staff/team content extraction.

CONTEXT:
- Page title: {page_info.get('title', 'Unknown')}
- Content detected: {'Yes' if page_info.get('hasContent') else 'No'}
- Social links found: {len(social_links)}

AVAILABLE INTERNAL PAGES:
{chr(10).join(internal_links)}

TASK: Select 3 BEST internal URLs for staff extraction
PRIORITY: team, about, staff, people, leadership, contact
AVOID: social media, external sites, product pages

Return JSON:
{{
  "selected_urls": ["url1", "url2", "url3"],
  "reasoning": "selection explanation"
}}"""

        try:
            from openai import OpenAI
            client = OpenAI(api_key=self.openai_key)
            
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=700,
                temperature=0.1
            )
            
            result_text = response.choices[0].message.content.strip()
            
            # Parse JSON response
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
        
        # Enhanced fallback selection
        return self._fallback_url_selection(all_links, domain)

    def _fallback_url_selection(self, all_links: list, domain: str) -> list:
        """🔄 Enhanced fallback URL selection"""
        
        staff_keywords = ["/team", "/about", "/staff", "/people", "/contact", "/leadership"]
        selected = []
        
        # Score and select URLs
        scored_urls = []
        for link in all_links:
            url = link.get('url', '').lower()
            text = link.get('text', '').lower()
            
            score = 0
            for keyword in staff_keywords:
                if keyword in url or keyword in text:
                    score += 10
            
            if score > 0:
                scored_urls.append((link.get('url', ''), score))
        
        # Sort by score and take top 3
        scored_urls.sort(key=lambda x: x[1], reverse=True)
        return [url for url, score in scored_urls[:3]]

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
        """🔍 Analyze single URL for staff and social media"""
        
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
                    
                    return staff, social
            
            return [], {}
            
        except Exception as e:
            print(f"    ❌ Content analysis failed: {e}")
            return [], {}

    def _gpt_extract_staff(self, content: str, url: str) -> list:
        """🧠 GPT-4o extracts staff from content"""
        
        if len(content) < 2000:
            return []
        
        domain = urlparse(url).netloc.replace('www.', '')
        
        # Extract staff-related sections
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
        """🔍 Validate if name looks like a real person"""
        if not name or len(name.strip()) < 3:
            return False
        
        name_parts = name.strip().split()
        
        if len(name_parts) < 2:
            return False
        
        company_name = domain.replace('.com', '').replace('.co.uk', '').replace('.ie', '')
        if name.lower().replace(' ', '') == company_name.lower().replace(' ', ''):
            return False
        
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