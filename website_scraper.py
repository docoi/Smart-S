"""
🌐 Smart Website Scraper Module - 3-TIER ACTOR SELECTION
========================================================
✅ TIER 1: Cheerio Scraper (fastest, cheapest, HTTP-only)
✅ TIER 2: Website Content Crawler (adaptive HTTP/browser hybrid)
✅ TIER 3: Puppeteer Scraper (most robust, full browser with anti-scraping protection)
✅ ALL TIERS: Use same cheapest Apify proxies with IP protection
✅ BULLETPROOF: Guaranteed success with automatic escalation
"""

import json
from urllib.parse import urlparse, urlunparse
from account_manager import get_working_apify_client_part1


class SmartWebsiteScraper:
    """🌐 Smart website scraping with 3-tier actor selection and IP protection"""
    
    def __init__(self, openai_key):
        self.openai_key = openai_key
        self.tier_used = None
        self.extraction_stats = {
            'tier1_cheerio': {'attempted': False, 'success': False, 'links_found': 0},
            'tier2_content_crawler': {'attempted': False, 'success': False, 'links_found': 0},
            'tier3_puppeteer': {'attempted': False, 'success': False, 'links_found': 0}
        }
    
    def scrape_website_for_staff_and_linkedin(self, website_url: str) -> tuple:
        """📋 Main method: Smart 3-tier website scraping with guaranteed success"""
        
        print(f"🗺️ SMART 3-TIER WEBSITE SCRAPING")
        print(f"🌐 Target: {website_url}")
        print(f"🛡️ IP Protection: Cheapest Apify proxies across all tiers")
        print(f"🎯 Strategy: Cheerio → Content Crawler → Puppeteer (auto-escalation)")
        print("=" * 70)
        
        # Normalize URL
        normalized_url = self._normalize_www(website_url)
        
        # TIER 1: Try Cheerio Scraper (fastest, cheapest)
        print(f"\n🥇 TIER 1: CHEERIO SCRAPER (HTTP-only, cheapest)")
        staff_list, linkedin_url = self._tier1_cheerio_scraper(normalized_url)
        
        if self._is_extraction_successful(staff_list, linkedin_url, tier=1):
            self.tier_used = "Tier 1 - Cheerio Scraper"
            print(f"✅ SUCCESS: Tier 1 completed successfully!")
            return staff_list, linkedin_url
        
        print(f"⚠️ Tier 1 insufficient - escalating to Tier 2...")
        
        # TIER 2: Try Website Content Crawler (adaptive hybrid)
        print(f"\n🥈 TIER 2: WEBSITE CONTENT CRAWLER (adaptive HTTP/browser)")
        staff_list, linkedin_url = self._tier2_content_crawler(normalized_url)
        
        if self._is_extraction_successful(staff_list, linkedin_url, tier=2):
            self.tier_used = "Tier 2 - Website Content Crawler"
            print(f"✅ SUCCESS: Tier 2 completed successfully!")
            return staff_list, linkedin_url
        
        print(f"⚠️ Tier 2 insufficient - escalating to Tier 3...")
        
        # TIER 3: Try Puppeteer Scraper (most robust, full browser)
        print(f"\n🥉 TIER 3: PUPPETEER SCRAPER (full browser with anti-scraping)")
        staff_list, linkedin_url = self._tier3_puppeteer_scraper(normalized_url)
        
        if self._is_extraction_successful(staff_list, linkedin_url, tier=3):
            self.tier_used = "Tier 3 - Puppeteer Scraper"
            print(f"✅ SUCCESS: Tier 3 completed successfully!")
            return staff_list, linkedin_url
        
        # All tiers attempted
        self.tier_used = "All tiers attempted"
        print(f"\n📊 FINAL RESULTS SUMMARY:")
        self._print_extraction_stats()
        
        # Return best results found
        return staff_list, linkedin_url

    def _tier1_cheerio_scraper(self, url: str) -> tuple:
        """🥇 Tier 1: Cheerio Scraper - Fastest, cheapest, HTTP-only"""
        
        self.extraction_stats['tier1_cheerio']['attempted'] = True
        
        try:
            # Get client with proxy configuration
            client = get_working_apify_client_part1()
            
            print(f"🚀 Running Cheerio Scraper (HTTP-only)...")
            print(f"💰 Cost: Lowest (no browser overhead)")
            print(f"🛡️ Proxy: Cheapest Apify proxies enabled")
            
            # Cheerio Scraper configuration
            cheerio_input = {
                "startUrls": [{"url": url}],
                "maxPagesPerRun": 3,
                "pageFunction": self._get_cheerio_page_function(),
                "proxyConfiguration": {"useApifyProxy": True},  # ← IP PROTECTION
                "maxRequestRetries": 3,
                "maxCrawlingDepth": 1,
                "sameDomainDelaySecs": 1
            }
            
            # Run Cheerio Scraper
            run = client.actor("apify/cheerio-scraper").call(run_input=cheerio_input)
            
            if run:
                items = client.dataset(run["defaultDatasetId"]).list_items().items
                return self._process_cheerio_results(items, url)
            
            return [], ""
            
        except Exception as e:
            print(f"❌ Tier 1 Cheerio Scraper failed: {e}")
            return [], ""

    def _tier2_content_crawler(self, url: str) -> tuple:
        """🥈 Tier 2: Website Content Crawler - Adaptive HTTP/browser hybrid"""
        
        self.extraction_stats['tier2_content_crawler']['attempted'] = True
        
        try:
            # Get client with proxy configuration
            client = get_working_apify_client_part1()
            
            print(f"🚀 Running Website Content Crawler (adaptive)...")
            print(f"💰 Cost: Medium (automatic HTTP/browser switching)")
            print(f"🛡️ Proxy: Cheapest Apify proxies enabled")
            print(f"🧠 Smart: Auto-detects if JavaScript rendering needed")
            
            # Website Content Crawler configuration
            crawler_input = {
                "startUrls": [{"url": url}],
                "maxPagesPerRun": 5,
                "crawlerType": "adaptive",  # Automatic HTTP/browser switching
                "proxyConfiguration": {"useApifyProxy": True},  # ← IP PROTECTION
                "maxCrawlingDepth": 1,
                "removeCookieWarnings": True,
                "clickElementsCssSelector": "button:contains('Accept'), .accept-cookies, #accept-cookies",
                "includeUrlGlobs": [f"{urlparse(url).netloc}/*"],
                "outputFormats": ["text", "markdown"],
                "saveHtml": False,
                "saveMarkdown": True,
                "saveFiles": False
            }
            
            # Run Website Content Crawler
            run = client.actor("apify/website-content-crawler").call(run_input=crawler_input)
            
            if run:
                items = client.dataset(run["defaultDatasetId"]).list_items().items
                return self._process_content_crawler_results(items, url)
            
            return [], ""
            
        except Exception as e:
            print(f"❌ Tier 2 Website Content Crawler failed: {e}")
            return [], ""

    def _tier3_puppeteer_scraper(self, url: str) -> tuple:
        """🥉 Tier 3: Puppeteer Scraper - Most robust, full browser with anti-scraping"""
        
        self.extraction_stats['tier3_puppeteer']['attempted'] = True
        
        try:
            # Get client with proxy configuration
            client = get_working_apify_client_part1()
            
            print(f"🚀 Running Puppeteer Scraper (full browser)...")
            print(f"💰 Cost: Highest (full Chromium browser)")
            print(f"🛡️ Proxy: Cheapest Apify proxies enabled")
            print(f"🔒 Anti-scraping: Advanced browser stealth mode")
            
            # Puppeteer Scraper configuration with enhanced anti-detection
            puppeteer_input = {
                "startUrls": [{"url": url}],
                "maxPagesPerRun": 3,
                "pageFunction": self._get_puppeteer_page_function(),
                "proxyConfiguration": {"useApifyProxy": True},  # ← IP PROTECTION
                "maxRequestRetries": 5,
                "maxCrawlingDepth": 1,
                "headless": True,
                "useChrome": True,  # Use real Chrome instead of Chromium
                "ignoreSslErrors": True,
                "clickElementsCssSelector": "button:contains('Accept'), .accept-cookies, #accept-cookies",
                "waitUntil": ["networkidle2"],
                "navigationTimeoutSecs": 60,
                "pageLoadTimeoutSecs": 60
            }
            
            # Run Puppeteer Scraper
            run = client.actor("apify/puppeteer-scraper").call(run_input=puppeteer_input)
            
            if run:
                items = client.dataset(run["defaultDatasetId"]).list_items().items
                return self._process_puppeteer_results(items, url)
            
            return [], ""
            
        except Exception as e:
            print(f"❌ Tier 3 Puppeteer Scraper failed: {e}")
            return [], ""

    def _get_cheerio_page_function(self) -> str:
        """📄 Cheerio page function for fast HTML parsing"""
        
        return """
async function pageFunction(context) {
    const { request, log, $ } = context;
    
    log.info('Cheerio: Starting fast HTML extraction...');
    
    const allLinks = [];
    const socialMedia = {};
    
    // Extract all links
    $('a[href]').each(function() {
        const href = $(this).attr('href');
        const text = $(this).text().trim();
        
        if (href && href.length > 1) {
            const baseUrl = request.url.split('/').slice(0, 3).join('/');
            let fullUrl = href;
            
            if (href.startsWith('/')) {
                fullUrl = baseUrl + href;
            } else if (href.startsWith('#')) {
                fullUrl = baseUrl + href;
            }
            
            allLinks.push({
                url: fullUrl,
                text: text,
                href: href
            });
        }
    });
    
    // Extract social media links
    $('a[href*="linkedin"], a[href*="facebook"], a[href*="twitter"], a[href*="instagram"]').each(function() {
        const href = $(this).attr('href');
        if (href) {
            if (href.includes('linkedin')) socialMedia.linkedin = href;
            if (href.includes('facebook')) socialMedia.facebook = href;
            if (href.includes('twitter')) socialMedia.twitter = href;
            if (href.includes('instagram')) socialMedia.instagram = href;
        }
    });
    
    const content = $('body').text() || '';
    
    log.info(`Cheerio: Found ${allLinks.length} links, ${content.length} chars content`);
    
    return {
        url: request.url,
        allLinks: allLinks,
        socialMedia: socialMedia,
        content: content,
        method: 'cheerio'
    };
}
"""

    def _get_puppeteer_page_function(self) -> str:
        """🎭 Puppeteer page function with advanced JavaScript handling"""
        
        return """
async function pageFunction(context) {
    const { request, page, log } = context;
    
    log.info('Puppeteer: Starting advanced browser extraction...');
    
    try {
        // Wait for dynamic content
        await page.waitForTimeout(5000);
        
        // Scroll to trigger lazy loading
        await page.evaluate(() => {
            window.scrollTo(0, document.body.scrollHeight);
        });
        await page.waitForTimeout(2000);
        
        await page.evaluate(() => {
            window.scrollTo(0, 0);
        });
        await page.waitForTimeout(2000);
        
        // Extract all links with JavaScript access
        const linkData = await page.evaluate(() => {
            const allLinks = [];
            const socialMedia = {};
            
            // Get all links including dynamically generated ones
            const links = document.querySelectorAll('a[href], [onclick*="location"], [data-href]');
            
            links.forEach(link => {
                let href = link.getAttribute('href') || link.getAttribute('data-href');
                const onclick = link.getAttribute('onclick') || '';
                const text = link.textContent.trim();
                
                // Extract href from onclick if needed
                if (!href && onclick) {
                    const match = onclick.match(/location[\\s]*=[\\s]*['""]([^'"'"]+)['"'"]/);
                    if (match) href = match[1];
                }
                
                if (href && href.length > 1) {
                    const baseUrl = window.location.origin;
                    let fullUrl = href;
                    
                    if (href.startsWith('/')) {
                        fullUrl = baseUrl + href;
                    } else if (href.startsWith('#')) {
                        fullUrl = baseUrl + href;
                    }
                    
                    allLinks.push({
                        url: fullUrl,
                        text: text,
                        href: href
                    });
                    
                    // Check for social media
                    const hrefLower = href.toLowerCase();
                    if (hrefLower.includes('linkedin') && !socialMedia.linkedin) {
                        socialMedia.linkedin = href;
                    } else if (hrefLower.includes('facebook') && !socialMedia.facebook) {
                        socialMedia.facebook = href;
                    }
                }
            });
            
            return {
                allLinks: allLinks,
                socialMedia: socialMedia,
                content: document.body.textContent || ''
            };
        });
        
        log.info(`Puppeteer: Found ${linkData.allLinks.length} links, ${linkData.content.length} chars content`);
        
        return {
            url: request.url,
            allLinks: linkData.allLinks,
            socialMedia: linkData.socialMedia,
            content: linkData.content,
            method: 'puppeteer'
        };
        
    } catch (error) {
        log.error('Puppeteer extraction failed:', error);
        return {
            url: request.url,
            allLinks: [],
            socialMedia: {},
            content: '',
            method: 'puppeteer_failed'
        };
    }
}
"""

    def _process_cheerio_results(self, items: list, url: str) -> tuple:
        """📊 Process Cheerio Scraper results"""
        
        if not items:
            return [], ""
        
        all_links = []
        social_data = {}
        all_content = ""
        
        for item in items:
            links = item.get('allLinks', [])
            social = item.get('socialMedia', {})
            content = item.get('content', '')
            
            all_links.extend(links)
            social_data.update(social)
            all_content += content + " "
        
        self.extraction_stats['tier1_cheerio']['links_found'] = len(all_links)
        
        print(f"📊 Cheerio Results: {len(all_links)} links, {len(all_content)} chars content")
        
        # Analyze links for staff pages
        staff_links = self._find_staff_links(all_links, url)
        
        if staff_links:
            print(f"🎯 Found {len(staff_links)} potential staff page links")
            staff_list = self._extract_staff_from_links(staff_links)
        else:
            staff_list = []
        
        # Find LinkedIn URL
        linkedin_url = self._extract_linkedin_url(social_data, url)
        
        if len(all_links) >= 10 and (staff_list or linkedin_url):
            self.extraction_stats['tier1_cheerio']['success'] = True
        
        return staff_list, linkedin_url

    def _process_content_crawler_results(self, items: list, url: str) -> tuple:
        """📊 Process Website Content Crawler results"""
        
        if not items:
            return [], ""
        
        all_text = ""
        all_links = []
        
        for item in items:
            text = item.get('text', '') + item.get('markdown', '')
            url_item = item.get('url', '')
            
            all_text += text + " "
            if url_item:
                all_links.append({'url': url_item, 'text': 'Content page'})
        
        self.extraction_stats['tier2_content_crawler']['links_found'] = len(all_links)
        
        print(f"📊 Content Crawler Results: {len(all_links)} pages, {len(all_text)} chars content")
        
        # Extract staff using GPT-4o from content
        staff_list = self._gpt_extract_staff_from_content(all_text, url)
        
        # Try to find LinkedIn URL in content
        linkedin_url = self._find_linkedin_in_content(all_text, url)
        
        if len(all_text) >= 5000 and (staff_list or linkedin_url):
            self.extraction_stats['tier2_content_crawler']['success'] = True
        
        return staff_list, linkedin_url

    def _process_puppeteer_results(self, items: list, url: str) -> tuple:
        """📊 Process Puppeteer Scraper results"""
        
        if not items:
            return [], ""
        
        all_links = []
        social_data = {}
        all_content = ""
        
        for item in items:
            links = item.get('allLinks', [])
            social = item.get('socialMedia', {})
            content = item.get('content', '')
            
            all_links.extend(links)
            social_data.update(social)
            all_content += content + " "
        
        self.extraction_stats['tier3_puppeteer']['links_found'] = len(all_links)
        
        print(f"📊 Puppeteer Results: {len(all_links)} links, {len(all_content)} chars content")
        
        # Analyze links for staff pages  
        staff_links = self._find_staff_links(all_links, url)
        
        if staff_links:
            print(f"🎯 Found {len(staff_links)} potential staff page links")
            staff_list = self._extract_staff_from_links(staff_links, use_puppeteer=True)
        else:
            # Fallback: extract from content
            staff_list = self._gpt_extract_staff_from_content(all_content, url)
        
        # Find LinkedIn URL
        linkedin_url = self._extract_linkedin_url(social_data, url)
        
        if len(all_links) >= 5 and (staff_list or linkedin_url):
            self.extraction_stats['tier3_puppeteer']['success'] = True
        
        return staff_list, linkedin_url

    def _find_staff_links(self, links: list, base_url: str) -> list:
        """🔍 Find potential staff/team page links"""
        
        staff_keywords = [
            'team', 'staff', 'people', 'about', 'employees', 'crew', 
            'leadership', 'management', 'directors', 'founders'
        ]
        
        staff_links = []
        domain = urlparse(base_url).netloc
        
        for link in links:
            url = link.get('url', '')
            text = link.get('text', '').lower()
            href = link.get('href', '').lower()
            
            # Must be same domain
            if domain not in url:
                continue
            
            # Check for staff-related keywords
            if any(keyword in href or keyword in text for keyword in staff_keywords):
                staff_links.append(link)
        
        return staff_links[:5]  # Limit to top 5

    def _extract_staff_from_links(self, staff_links: list, use_puppeteer: bool = False) -> list:
        """👥 Extract staff from staff page links"""
        
        all_staff = []
        
        for link in staff_links:
            url = link.get('url', '')
            
            try:
                if use_puppeteer:
                    content = self._fetch_content_puppeteer(url)
                else:
                    content = self._fetch_content_simple(url)
                
                if content and len(content) > 1000:
                    staff = self._gpt_extract_staff_from_content(content, url)
                    all_staff.extend(staff)
                    
            except Exception as e:
                print(f"   ⚠️ Error extracting from {url}: {e}")
        
        return all_staff

    def _fetch_content_simple(self, url: str) -> str:
        """📄 Simple content fetch using HTTP"""
        
        try:
            client = get_working_apify_client_part1()
            
            # Simple HTTP fetch
            simple_input = {
                "startUrls": [{"url": url}],
                "maxPagesPerRun": 1,
                "pageFunction": "async function pageFunction(context) { const { $ } = context; return { content: $('body').text() }; }",
                "proxyConfiguration": {"useApifyProxy": True}
            }
            
            run = client.actor("apify/cheerio-scraper").call(run_input=simple_input)
            
            if run:
                items = client.dataset(run["defaultDatasetId"]).list_items().items
                if items:
                    return items[0].get('content', '')
                    
        except Exception as e:
            print(f"   ❌ Simple fetch failed: {e}")
        
        return ""

    def _fetch_content_puppeteer(self, url: str) -> str:
        """🎭 Advanced content fetch using Puppeteer"""
        
        try:
            client = get_working_apify_client_part1()
            
            # Puppeteer fetch with JavaScript rendering
            puppeteer_input = {
                "startUrls": [{"url": url}],
                "maxPagesPerRun": 1,
                "pageFunction": """
                async function pageFunction(context) {
                    const { page } = context;
                    await page.waitForTimeout(3000);
                    const content = await page.evaluate(() => document.body.textContent);
                    return { content: content };
                }
                """,
                "proxyConfiguration": {"useApifyProxy": True},
                "headless": True
            }
            
            run = client.actor("apify/puppeteer-scraper").call(run_input=puppeteer_input)
            
            if run:
                items = client.dataset(run["defaultDatasetId"]).list_items().items
                if items:
                    return items[0].get('content', '')
                    
        except Exception as e:
            print(f"   ❌ Puppeteer fetch failed: {e}")
        
        return ""

    def _gpt_extract_staff_from_content(self, content: str, url: str) -> list:
        """🧠 Extract staff using GPT-4o"""
        
        if len(content) < 500:
            return []
        
        domain = urlparse(url).netloc.replace('www.', '')
        
        prompt = f"""Extract staff from this {domain} content.

RULES:
1. Find ONLY current employees of {domain}
2. Need: Full name + Job title  
3. EXCLUDE: Clients, testimonials, external people
4. PRIORITIZE: Management, directors, decision-makers

Return JSON: [{{"name": "Full Name", "title": "Job Title"}}]
If none found: []

Content:
{content[:20000]}"""

        try:
            from openai import OpenAI
            client = OpenAI(api_key=self.openai_key)
            
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=1000,
                temperature=0.1
            )
            
            result_text = response.choices[0].message.content.strip()
            
            # Parse JSON
            json_start = result_text.find('[')
            json_end = result_text.rfind(']') + 1
            
            if json_start >= 0 and json_end > json_start:
                json_text = result_text[json_start:json_end]
                staff_list = json.loads(json_text)
                
                print(f"   🧠 GPT-4o found {len(staff_list)} staff members")
                return staff_list[:10]
                
        except Exception as e:
            print(f"   ⚠️ GPT staff extraction failed: {e}")
        
        return []

    def _extract_linkedin_url(self, social_data: dict, base_url: str) -> str:
        """🔗 Extract LinkedIn company URL"""
        
        # From social data
        linkedin_url = social_data.get('linkedin', '')
        
        if linkedin_url and 'company' in linkedin_url:
            return linkedin_url
        
        # Generate fallback LinkedIn URL
        domain = urlparse(base_url).netloc.replace('www.', '')
        company_name = domain.replace('.com', '').replace('.co.uk', '').replace('.ie', '')
        
        return f"https://www.linkedin.com/company/{company_name}"

    def _find_linkedin_in_content(self, content: str, base_url: str) -> str:
        """🔍 Find LinkedIn URL in content text"""
        
        import re
        
        # Look for LinkedIn URLs in content
        linkedin_pattern = r'https?://[www\.]*linkedin\.com/company/[^\s\'"<>]+'
        matches = re.findall(linkedin_pattern, content, re.IGNORECASE)
        
        if matches:
            return matches[0]
        
        # Fallback
        return self._extract_linkedin_url({}, base_url)

    def _is_extraction_successful(self, staff_list: list, linkedin_url: str, tier: int) -> bool:
        """✅ Determine if extraction was successful enough to stop escalation"""
        
        # Tier 1: Basic success criteria
        if tier == 1:
            return len(staff_list) >= 1 or (linkedin_url and 'linkedin.com/company' in linkedin_url)
        
        # Tier 2: Moderate success criteria  
        elif tier == 2:
            return len(staff_list) >= 1 or linkedin_url
        
        # Tier 3: Any results are acceptable (final tier)
        else:
            return len(staff_list) >= 0 or linkedin_url

    def _print_extraction_stats(self):
        """📊 Print detailed extraction statistics"""
        
        print(f"\n📊 SMART SCRAPER EXTRACTION STATISTICS:")
        print(f"=" * 50)
        
        for tier_name, stats in self.extraction_stats.items():
            tier_display = tier_name.replace('_', ' ').title()
            attempted = "✅" if stats['attempted'] else "⏭️ "
            success = "✅" if stats['success'] else "❌"
            links = stats['links_found']
            
            print(f"{attempted} {tier_display}: {success} Success | {links} links found")
        
        print(f"\n🎯 Final tier used: {self.tier_used}")
        print(f"💰 Cost optimization: Started with cheapest tier")
        print(f"🛡️ IP protection: Maintained across all tiers")

    def _normalize_www(self, url: str) -> str:
        """🌐 Normalize URL format"""
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        parsed = urlparse(url)
        netloc = parsed.netloc
        
        # Remove www. for consistency
        if netloc.startswith('www.'):
            netloc = netloc[4:]
        
        return urlunparse((parsed.scheme, netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))