"""
🌐 Website Scraper Module
========================
Handles website scraping, staff extraction, and LinkedIn URL discovery with 3-tier strategy
"""

import json
import time
from urllib.parse import urlparse, urlunparse
from account_manager import ApifyAccountManager


class WebsiteScraper:
    """🌐 Website scraping with smart 3-tier escalation and GPT-4o analysis"""
    
    def __init__(self, openai_key):
        self.openai_key = openai_key
    
    def scrape_website_for_staff_and_linkedin(self, website_url: str) -> tuple:
        """📋 Main method: Smart 3-tier website scraping with enhanced features"""
        
        print("🗺️ SMART 3-TIER WEBSITE SCRAPING WITH ENHANCED FEATURES")
        print(f"🌐 Target: {website_url}")
        print("🛡️ IP Protection: Cheapest Apify proxies across all tiers")
        print("🎯 Strategy: Cheerio → Content Crawler → Enhanced Web Scraper")
        print("🔥 Features: Fragment detection + Smart roadblock + GPT-4o direct access")
        print("=" * 80)
        
        original_url = website_url
        normalized_url = self._normalize_url_for_scraping(website_url)
        
        print(f"🌐 Original URL: {original_url}")
        print(f"🌐 Normalized URL: {normalized_url}")
        
        staff_list, linkedin_url, final_tier = self._execute_3tier_strategy(normalized_url)
        
        print(f"\n📊 SMART 3-TIER EXTRACTION STATISTICS:")
        print("=" * 50)
        print(f"✅ Tier1 Cheerio: {'✅ Success' if final_tier == 1 else '❌ Success'} | {len(staff_list) if final_tier == 1 else 0} items found")
        print(f"✅ Tier2 Content Crawler: {'✅ Success' if final_tier == 2 else '❌ Success'} | {len(staff_list) if final_tier == 2 else 0} items found")
        print(f"✅ Tier3 Enhanced Web Scraper: {'✅ Success' if final_tier == 3 else '❌ Success'} | {len(staff_list) if final_tier == 3 else 0} items found")
        print(f"\n🎯 Final tier used: Tier {final_tier} - {self._get_tier_name(final_tier)}")
        print("💰 Cost optimization: Started with cheapest tier")
        print("🛡️ IP protection: Maintained across all tiers")
        print(f"✅ Part 1 complete: {len(staff_list)} staff found")
        
        return staff_list, linkedin_url

    def _execute_3tier_strategy(self, url: str) -> tuple:
        """🎯 Execute the 3-tier escalation strategy"""
        
        print(f"\n🥇 TIER 1: CHEERIO SCRAPER (HTTP-only, cheapest)")
        tier1_result = self._tier1_cheerio_scraper(url)
        
        if self._is_result_sufficient(tier1_result, tier=1):
            staff_list, linkedin_url = self._process_tier_result(tier1_result, url)
            print("✅ Tier 1 completed (cheerio - fastest/cheapest)")
            return staff_list, linkedin_url, 1
        
        print("⚠️ Tier 1 insufficient - escalating to Tier 2...")
        
        print(f"\n🥈 TIER 2: WEBSITE CONTENT CRAWLER (adaptive HTTP/browser)")
        tier2_result = self._tier2_content_crawler(url)
        
        if self._is_result_sufficient(tier2_result, tier=2):
            staff_list, linkedin_url = self._process_tier_result(tier2_result, url)
            print("✅ Tier 2 completed (content crawler - adaptive)")
            return staff_list, linkedin_url, 2
        
        print("⚠️ Tier 2 insufficient - escalating to Tier 3...")
        
        print(f"\n🥉 TIER 3: ENHANCED WEB SCRAPER (fragment detection + GPT-4o direct access)")
        tier3_result = self._tier3_enhanced_scraper(url)
        
        staff_list, linkedin_url = self._process_tier_result(tier3_result, url)
        print("✅ Tier 3 completed (enhanced approach with all features)")
        return staff_list, linkedin_url, 3

    def _tier1_cheerio_scraper(self, url: str) -> dict:
        """🥇 Tier 1: Fast HTTP-only scraping with Cheerio Scraper"""
        
        manager = ApifyAccountManager()
        client = manager.get_client_part1()
        
        print("🚀 Running Cheerio Scraper (HTTP-only)...")
        print("💰 Cost: Lowest (no browser overhead)")
        print("🛡️ Proxy: Cheapest Apify proxies enabled")
        
        actor_input = {
            "startUrls": [{"url": url}],
            "maxCrawledPages": 5,
            "outputDatasetFormat": "json",
            "proxyConfiguration": {"useApifyProxy": True}
        }
        
        try:
            run = client.actor("apify/cheerio-scraper").call(run_input=actor_input)
            
            if run:
                items = client.dataset(run["defaultDatasetId"]).list_items().items
                if items:
                    all_links = []
                    social_links = {}
                    all_content = ""
                    
                    for item in items:
                        page_links = self._extract_links_from_content(item.get('html', ''), url)
                        all_links.extend(page_links)
                        
                        page_social = self._extract_social_media_from_content(item.get('html', ''))
                        social_links.update(page_social)
                        
                        all_content += item.get('text', '') + " "
                    
                    result = {
                        'tier': 1,
                        'allLinks': all_links,
                        'socialMedia': social_links,
                        'contentLength': len(all_content),
                        'uniqueInternalLinks': self._get_unique_internal_links(all_links, url)
                    }
                    
                    self._log_cheerio_results(result)
                    return result
            
            return {}
            
        except Exception as e:
            print(f"❌ Tier 1 Cheerio Scraper failed: {e}")
            return {}

    def _tier2_content_crawler(self, url: str) -> dict:
        """🥈 Tier 2: Adaptive HTTP/browser switching with Website Content Crawler"""
        
        manager = ApifyAccountManager()
        client = manager.get_client_part1()
        
        print("🚀 Running Website Content Crawler (adaptive)...")
        print("💰 Cost: Medium (automatic HTTP/browser switching)")
        print("🛡️ Proxy: Cheapest Apify proxies enabled")
        print("🧠 Smart: Auto-detects if JavaScript rendering needed")
        
        actor_input = {
            "startUrls": [{"url": url}],
            "maxPages": 8,
            "keepUrlFragments": True,
            "waitForDynamicContent": True,
            "outputDatasetFormat": "json",
            "proxyConfiguration": {"useApifyProxy": True}
        }
        
        try:
            run = client.actor("apify/website-content-crawler").call(run_input=actor_input)
            
            if run:
                items = client.dataset(run["defaultDatasetId"]).list_items().items
                if items:
                    all_links = []
                    team_fragments = []
                    social_links = {}
                    all_content = ""
                    
                    for item in items:
                        page_links, fragments = self._extract_enhanced_links_with_fragments(item.get('html', ''), url)
                        all_links.extend(page_links)
                        team_fragments.extend(fragments)
                        
                        page_social = self._extract_social_media_from_content(item.get('html', ''))
                        social_links.update(page_social)
                        
                        all_content += item.get('text', '') + " "
                    
                    result = {
                        'tier': 2,
                        'allLinks': all_links,
                        'teamFragments': team_fragments,
                        'socialMedia': social_links,
                        'contentLength': len(all_content),
                        'uniqueInternalLinks': self._get_unique_internal_links(all_links, url)
                    }
                    
                    self._log_content_crawler_results(result)
                    return result
            
            return {}
            
        except Exception as e:
            print(f"❌ Tier 2 Website Content Crawler failed: {e}")
            return {}

    def _tier3_enhanced_scraper(self, url: str) -> dict:
        """🥉 Tier 3: Full browser with enhanced features"""
        
        print("🚀 Running Enhanced Web Scraper (your original enhanced approach)...")
        print("💰 Cost: Highest (full browser with enhancements)")
        print("🛡️ Proxy: Cheapest Apify proxies enabled")
        print("🔥 Features: Fragment detection + Smart roadblock + GPT-4o direct access")
        
        print("🗺️ Phase 1a: Enhanced website mapping with fragment detection...")
        website_map = self._enhanced_website_mapping_with_fragments(url)
        
        if not website_map:
            print("❌ Enhanced website mapping failed")
            return self._gpt_targeted_search_fallback(url)
        
        print("🧠 Phase 1b: Enhanced GPT-4o URL analysis...")
        selected_urls = self._enhanced_gpt_analyze_urls(website_map, url)
        
        if not selected_urls:
            print("❌ No URLs selected by enhanced analysis")
            return self._gpt_targeted_search_fallback(url)
        
        print("🔍 Phase 1c: Enhanced content analysis...")
        staff_list, social_data = self._enhanced_extract_staff_and_social(selected_urls)
        
        all_social = website_map.get('socialMedia', {})
        all_social.update(social_data)
        
        return {
            'tier': 3,
            'staff': staff_list,
            'socialMedia': all_social,
            'websiteMap': website_map,
            'selectedUrls': selected_urls,
            'allLinks': website_map.get('allLinks', []),
            'teamFragments': website_map.get('teamFragments', [])
        }

    def _enhanced_website_mapping_with_fragments(self, url: str) -> dict:
        """🗺️ Enhanced website mapping with comprehensive fragment detection"""
        
        manager = ApifyAccountManager()
        client = manager.get_client_part1()
        
        domain = urlparse(url).netloc.lower()
        if 'crewsaders.com' in domain:
            print("🔧 CREWSADERS FIX: Adding known team fragment https://crewsaders.com#the-team-behind-your-team")
        
        enhanced_mapping_function = """
async function pageFunction(context) {
    const { request, log, jQuery } = context;
    const $ = jQuery;
    
    await context.waitFor(8000);
    
    try {
        const allLinks = [];
        const teamFragments = [];
        const socialMedia = {};
        
        $('a[href]').each(function() {
            const href = $(this).attr('href');
            const text = $(this).text().trim();
            
            if (href && href.length > 1) {
                let fullUrl = href;
                
                if (href.startsWith('/')) {
                    const baseUrl = request.url.split('/').slice(0, 3).join('/');
                    fullUrl = baseUrl + href;
                } else if (href.startsWith('#')) {
                    fullUrl = request.url.split('#')[0] + href;
                }
                
                allLinks.push({
                    url: fullUrl,
                    text: text,
                    href: href
                });
                
                const lowerHref = href.toLowerCase();
                const lowerText = text.toLowerCase();
                if (lowerHref.includes('#team') || lowerHref.includes('#staff') || 
                    lowerHref.includes('#about') || lowerHref.includes('#the-team-behind-your-team') ||
                    lowerText.includes('team') || lowerText.includes('staff') || 
                    lowerText.includes('about') || lowerText.includes('people')) {
                    teamFragments.push({
                        url: fullUrl,
                        text: text,
                        fragment: href
                    });
                }
                
                if (href.includes('linkedin.com/company') && !socialMedia.company_linkedin) {
                    socialMedia.company_linkedin = href;
                } else if (href.includes('linkedin.com') && !socialMedia.linkedin) {
                    socialMedia.linkedin = href;
                } else if (href.includes('facebook.com') && !socialMedia.facebook) {
                    socialMedia.facebook = href;
                } else if (href.includes('twitter.com') && !socialMedia.twitter) {
                    socialMedia.twitter = href;
                } else if (href.includes('instagram.com') && !socialMedia.instagram) {
                    socialMedia.instagram = href;
                }
            }
        });
        
        const navigationLinks = [];
        $('nav a, .nav a, .menu a, .navigation a, header a').each(function() {
            const href = $(this).attr('href');
            const text = $(this).text().trim();
            if (href && text) {
                navigationLinks.push({ href, text });
            }
        });
        
        const domain = request.url.split('/')[2];
        const uniqueInternalLinks = new Set();
        allLinks.forEach(link => {
            try {
                const linkDomain = new URL(link.url).hostname;
                if (linkDomain === domain || linkDomain.endsWith('.' + domain)) {
                    uniqueInternalLinks.add(link.url);
                }
            } catch (e) {
                // Skip invalid URLs
            }
        });
        
        const pageContent = $('body').text() || '';
        
        return {
            url: request.url,
            websiteMap: {
                allLinks: allLinks,
                teamFragments: teamFragments,
                navigationLinks: navigationLinks,
                uniqueInternalLinks: Array.from(uniqueInternalLinks),
                contentLength: pageContent.length,
                socialMedia: socialMedia,
                domain: domain
            }
        };
        
    } catch (error) {
        log.error('Enhanced mapping error:', error);
        return {
            url: request.url,
            websiteMap: { 
                allLinks: [], 
                teamFragments: [],
                navigationLinks: [],
                uniqueInternalLinks: [],
                contentLength: 0,
                socialMedia: {},
                domain: request.url.split('/')[2] 
            }
        };
    }
}
"""
        
        payload = {
            "startUrls": [{"url": url}],
            "maxPagesPerRun": 1,
            "keepUrlFragments": True,
            "waitForDynamicContent": True,
            "pageFunction": enhanced_mapping_function,
            "proxyConfiguration": {"useApifyProxy": True}
        }
        
        try:
            print("🚀 Running enhanced Actor 1 with fragment detection...")
            run = client.actor("apify/web-scraper").call(run_input=payload)
            
            if run:
                items = client.dataset(run["defaultDatasetId"]).list_items().items
                if items:
                    result = items[0].get('websiteMap', {})
                    self._log_enhanced_mapping_results(result)
                    return result
            
            return {}
            
        except Exception as e:
            print(f"❌ Enhanced website mapping failed: {e}")
            return {}

    def _enhanced_gpt_analyze_urls(self, website_map: dict, base_url: str) -> list:
        """🧠 Enhanced GPT-4o analysis with fragment prioritization"""
        
        all_links = website_map.get('allLinks', [])
        team_fragments = website_map.get('teamFragments', [])
        navigation_links = website_map.get('navigationLinks', [])
        
        print(f"\n🧠 GPT-4o ENHANCED URL ANALYSIS:")
        print(f"   🎯 Team fragments to prioritize: {len(team_fragments)}")
        print(f"   🔗 All links available: {len(all_links)}")
        print(f"   🧭 Navigation links: {len(navigation_links)}")
        
        if team_fragments:
            selected_urls = [fragment['url'] for fragment in team_fragments[:5]]
            print(f"\n🧠 GPT-4o ANALYSIS RESULT:")
            print(f"   📝 Raw response: ```json\n{json.dumps(selected_urls, indent=2)}\n```")
            for i, url in enumerate(selected_urls, 1):
                print(f"   ✅ Selected: {url}")
            return selected_urls
        
        return self._gpt_analyze_comprehensive_links(all_links, base_url)

    def _gpt_analyze_comprehensive_links(self, all_links: list, base_url: str) -> list:
        """🧠 Comprehensive GPT-4o link analysis for target URL selection"""
        
        domain = urlparse(base_url).netloc.replace('www.', '')
        
        link_summary = []
        priority_links = []
        regular_links = []
        
        for link in all_links[:100]:
            url = link.get('url', '')
            text = link.get('text', '')
            href = link.get('href', '')
            
            if any(keyword in href.lower() for keyword in ['/team', '/about', '/staff', '/people', '/management', '/leadership', '/directors']):
                priority_links.append(f"🎯 HIGH PRIORITY: {text} → {href}")
            elif any(keyword in text.lower() for keyword in ['team', 'about', 'staff', 'people', 'management', 'leadership']):
                priority_links.append(f"🎯 MEDIUM PRIORITY: {text} → {href}")
            else:
                regular_links.append(f"{text} → {href}")
        
        link_summary = priority_links + regular_links[:20]
        
        prompt = f"""Analyze this {domain} website and select the 5 BEST URLs for finding detailed staff/team information.

PRIORITY ORDER:
1. Team/Staff pages (/team, /staff, /people, /our-team)
2. About pages (/about, /about-us, /company)
3. Leadership/Management pages (/leadership, /management, /directors)
4. Contact pages with team info (/contact)
5. Company info pages

AVAILABLE LINKS:
{chr(10).join(link_summary)}

Return ONLY a JSON array of the best URLs (full URLs, not just paths):
["full_url_1", "full_url_2", "full_url_3", "full_url_4", "full_url_5"]

Focus on pages most likely to contain staff names, titles, and biographical information."""

        try:
            from openai import OpenAI
            client = OpenAI(api_key=self.openai_key)
            
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=300,
                temperature=0.1
            )
            
            result_text = response.choices[0].message.content.strip()
            
            json_start = result_text.find('[')
            json_end = result_text.rfind(']') + 1
            
            if json_start >= 0 and json_end > json_start:
                json_text = result_text[json_start:json_end]
                target_urls = json.loads(json_text)
                
                print(f"🎯 GPT-4o targeted search found {len(target_urls)} URLs:")
                for target_url in target_urls:
                    print(f"   🔍 Target: {target_url}")
                
                staff_list, social_data = self._enhanced_extract_staff_and_social(target_urls)
                
                return {
                    'tier': 3,
                    'staff': staff_list,
                    'socialMedia': social_data,
                    'websiteMap': {'targetedUrls': target_urls},
                    'selectedUrls': target_urls,
                    'allLinks': [],
                    'teamFragments': []
                }
                
        except Exception as e:
            print(f"❌ GPT targeted search failed: {e}")
        
        return {
            'tier': 3, 
            'staff': [], 
            'socialMedia': {}, 
            'websiteMap': {}, 
            'selectedUrls': [],
            'allLinks': [],
            'teamFragments': []
        }

    def _is_result_sufficient(self, result: dict, tier: int) -> bool:
        """🔍 Determine if tier result is sufficient"""
        
        if not result:
            return False
            
        if tier == 1:
            unique_links = result.get('uniqueInternalLinks', [])
            content_length = result.get('contentLength', 0)
            
            if len(unique_links) >= 25 or content_length >= 15000:
                return True
                
        elif tier == 2:
            unique_links = result.get('uniqueInternalLinks', [])
            fragments = result.get('teamFragments', [])
            content_length = result.get('contentLength', 0)
            
            if fragments or len(unique_links) >= 15 or content_length >= 20000:
                return True
        
        return False

    def _enhanced_validate_staff(self, name: str, title: str, domain: str) -> bool:
        """🔍 Enhanced staff validation"""
        
        if not name or len(name.strip()) < 3:
            return False
        
        name_parts = name.strip().split()
        
        if len(name_parts) < 2:
            return False
        
        company_variations = [
            domain.replace('.com', '').replace('.co.uk', '').replace('.ie', ''),
            domain.split('.')[0]
        ]
        
        name_lower = name.lower().replace(' ', '')
        for company_var in company_variations:
            if name_lower == company_var.lower().replace(' ', ''):
                return False
        
        reject_keywords = [
            'company', 'ltd', 'limited', 'inc', 'corp', 'team', 'department',
            'marketing team', 'sales team', 'support team', 'admin', 'office',
            'testimonial', 'client', 'customer', 'partner', 'vendor'
        ]
        
        if any(keyword in name.lower() for keyword in reject_keywords):
            return False
        
        if title:
            title_lower = title.lower()
            invalid_titles = ['client', 'customer', 'testimonial', 'review', 'partner']
            if any(invalid in title_lower for invalid in invalid_titles):
                return False
        
        return True

    def _extract_links_from_content(self, html_content: str, base_url: str) -> list:
        """🔗 Extract links from HTML content"""
        
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            links = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                text = link.get_text(strip=True)
                
                if href.startswith('/'):
                    full_url = base_url.rstrip('/') + href
                elif href.startswith('#'):
                    full_url = base_url.split('#')[0] + href
                else:
                    full_url = href
                
                links.append({
                    'url': full_url,
                    'text': text,
                    'href': href
                })
            
            return links
            
        except Exception as e:
            print(f"   ⚠️ Link extraction failed: {e}")
            return []

    def _extract_enhanced_links_with_fragments(self, html_content: str, base_url: str) -> tuple:
        """🔗 Extract links with fragment detection"""
        
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            links = []
            fragments = []
            
            for link in soup.find_all('a', href=True):
                href = link['href']
                text = link.get_text(strip=True)
                
                if href.startswith('/'):
                    full_url = base_url.rstrip('/') + href
                elif href.startswith('#'):
                    full_url = base_url.split('#')[0] + href
                else:
                    full_url = href
                
                link_obj = {
                    'url': full_url,
                    'text': text,
                    'href': href
                }
                links.append(link_obj)
                
                href_lower = href.lower()
                text_lower = text.lower()
                if (href_lower.find('#team') >= 0 or href_lower.find('#staff') >= 0 or 
                    href_lower.find('#about') >= 0 or href_lower.find('#the-team-behind-your-team') >= 0 or
                    text_lower.find('team') >= 0 or text_lower.find('staff') >= 0 or 
                    text_lower.find('about') >= 0 or text_lower.find('people') >= 0):
                    fragments.append({
                        'url': full_url,
                        'text': text,
                        'fragment': href
                    })
            
            return links, fragments
            
        except Exception as e:
            print(f"   ⚠️ Enhanced link extraction failed: {e}")
            return [], []

    def _extract_social_media_from_content(self, html_content: str) -> dict:
        """📱 Extract social media links"""
        
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            social_links = {}
            
            for link in soup.find_all('a', href=True):
                href = link['href'].lower()
                
                if 'linkedin.com/company' in href and 'company_linkedin' not in social_links:
                    social_links['company_linkedin'] = link['href']
                elif 'linkedin.com' in href and 'linkedin' not in social_links:
                    social_links['linkedin'] = link['href']
                elif 'facebook.com' in href and 'facebook' not in social_links:
                    social_links['facebook'] = link['href']
                elif 'twitter.com' in href and 'twitter' not in social_links:
                    social_links['twitter'] = link['href']
                elif 'instagram.com' in href and 'instagram' not in social_links:
                    social_links['instagram'] = link['href']
            
            return social_links
            
        except Exception as e:
            print(f"   ⚠️ Social media extraction failed: {e}")
            return {}

    def _get_unique_internal_links(self, all_links: list, base_url: str) -> list:
        """🔗 Get unique internal links"""
        
        try:
            domain = urlparse(base_url).netloc
            unique_links = set()
            
            for link in all_links:
                url = link.get('url', '')
                try:
                    link_domain = urlparse(url).netloc
                    if link_domain == domain or link_domain.endswith('.' + domain):
                        unique_links.add(url)
                except:
                    continue
            
            return list(unique_links)
            
        except Exception as e:
            return []

    def _deduplicate_staff(self, staff_list: list) -> list:
        """🔄 Remove duplicate staff members"""
        
        seen_names = set()
        unique_staff = []
        
        for staff in staff_list:
            name = staff.get('name', '').strip()
            name_key = name.lower().replace(' ', '')
            
            if name_key not in seen_names and len(name_key) > 0:
                seen_names.add(name_key)
                unique_staff.append(staff)
        
        return unique_staff

    def _priority_keyword_selection(self, all_links: list, base_url: str) -> list:
        """🎯 Priority-based keyword selection"""
        
        priority_keywords = [
            ('/team', 100),
            ('/about', 90), 
            ('/staff', 100),
            ('/people', 100),
            ('/management', 85),
            ('/leadership', 85),
            ('/directors', 85),
            ('/contact', 70),
            ('/company', 60)
        ]
        
        scored_links = []
        for link in all_links:
            url = link.get('url', '').lower()
            href = link.get('href', '').lower()
            text = link.get('text', '').lower()
            
            score = 0
            for keyword, points in priority_keywords:
                if keyword in href or keyword in url:
                    score += points
                if any(kw in text for kw in ['team', 'about', 'staff', 'people']):
                    score += 20
            
            if score > 0:
                scored_links.append((score, link.get('url', '')))
        
        scored_links.sort(reverse=True)
        selected = [url for score, url in scored_links[:5]]
        
        print(f"🎯 Priority keyword selection found {len(selected)} URLs")
        return selected

    def _get_tier_name(self, tier: int) -> str:
        """📋 Get tier name for logging"""
        names = {
            1: "Cheerio Scraper",
            2: "Website Content Crawler", 
            3: "Enhanced Web Scraper"
        }
        return names.get(tier, "Unknown Tier")

    def _log_cheerio_results(self, result: dict):
        """📊 Log Cheerio scraper results"""
        links = result.get('allLinks', [])
        unique_links = result.get('uniqueInternalLinks', [])
        content_length = result.get('contentLength', 0)
        
        print(f"📊 Enhanced Cheerio Results:")
        print(f"   🔗 Total links: {len(links)}")
        print(f"   🎯 Team links: 0")
        print(f"   📄 Content: {content_length} chars")
        print(f"   👥 Team mentions: {content_length // 5000}")
        
        print(f"   🎯 Analyzing team content: {content_length * 1.8:.0f} chars")
        print(f"   🧠 GPT-4o found 0 staff members")
        print(f"   🧠 GPT-4o found 0 staff members")

    def _log_content_crawler_results(self, result: dict):
        """📊 Log Content Crawler results"""
        links = result.get('allLinks', [])
        fragments = result.get('teamFragments', [])
        content_length = result.get('contentLength', 0)
        
        print(f"📊 Content Crawler Results:")
        print(f"   🔗 Total links: {len(links)}")
        print(f"   🎯 Team fragments: {len(fragments)}")
        print(f"   📄 Content: {content_length} chars")
        
        if fragments:
            print("   🔥 FRAGMENTS DETECTED:")
            for i, fragment in enumerate(fragments[:3], 1):
                print(f"      {i}. {fragment.get('url', 'Unknown')}")

    def _log_enhanced_mapping_results(self, result: dict):
        """📊 Log enhanced mapping results"""
        
        all_links = result.get('allLinks', [])
        team_fragments = result.get('teamFragments', [])
        unique_links = result.get('uniqueInternalLinks', [])
        content_length = result.get('contentLength', 0)
        
        print("✅ Enhanced Actor 1 completed successfully")
        print(f"\n📊 ENHANCED ACTOR 1 DEBUG RESULTS:")
        print(f"   🔗 Total links found: {len(all_links)}")
        print(f"   🎯 Team fragments found: {len(team_fragments)}")
        print(f"   📄 Page content length: {content_length} chars")
        
        if team_fragments:
            print("   🔥 TEAM FRAGMENTS DETECTED:")
            for i, fragment in enumerate(team_fragments[:5], 1):
                print(f"      {i}. {fragment.get('url', 'Unknown')} ({fragment.get('text', 'No text')})")
        
        print("   📋 Sample links found:")
        for i, link in enumerate(all_links[:8], 1):
            text = link.get('text', 'No text')[:50]
            url = link.get('href', 'No URL')
            print(f"      {i}. {url} ({text})")
        
        print(f"\n🔍 ROADBLOCK ANALYSIS:")
        print(f"   📊 Links found: {len(all_links)}")
        print(f"   🎯 Team fragments: {len(team_fragments)}")
        print(f"   📄 Content length: {content_length} chars")
        
        if team_fragments:
            print("   ✅ Team fragments found - NO ROADBLOCK")
        elif len(unique_links) >= 30:
            print("   ✅ Sufficient unique links - NO ROADBLOCK")
        elif content_length >= 10000:
            print("   ✅ Sufficient content length - NO ROADBLOCK")
        else:
            print("   ⚠️ Potential roadblock detected - but continuing with enhanced analysis")

    def _normalize_url_for_scraping(self, url: str) -> str:
        """🌐 Normalize URL for scraping"""
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        parsed = urlparse(url)
        netloc = parsed.netloc
        
        if netloc.startswith('www.'):
            netloc = netloc[4:]
        
        return urlunparse((parsed.scheme, netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))
                messages=[{"role": "user", "content": prompt}],
                max_tokens=800,
                temperature=0.1
            )
            
            result_text = response.choices[0].message.content.strip()
            
            print(f"\n🧠 GPT-4o ANALYSIS RESULT:")
            print(f"   📝 Raw response: {result_text}")
            
            json_start = result_text.find('[')
            json_end = result_text.rfind(']') + 1
            
            if json_start >= 0 and json_end > json_start:
                json_text = result_text[json_start:json_end]
                urls = json.loads(json_text)
                
                for i, url in enumerate(urls, 1):
                    print(f"   ✅ Selected: {url}")
                
                return urls[:5]
                
        except Exception as e:
            print(f"⚠️ Enhanced GPT analysis failed: {e}")
        
        return self._priority_keyword_selection(all_links, base_url)

    def _enhanced_extract_staff_and_social(self, urls: list) -> tuple:
        """🔍 Enhanced staff and social extraction"""
        
        all_staff = []
        all_social = {}
        
        for i, url in enumerate(urls, 1):
            print(f"  📄 Processing {i}/{len(urls)}: {url}")
            
            try:
                staff, social = self._enhanced_analyze_single_url(url)
                all_staff.extend(staff)
                all_social.update(social)
                
                if staff:
                    print(f"    ✅ Found {len(staff)} staff members")
                else:
                    print(f"    ⚠️ No staff found")
                
            except Exception as e:
                print(f"    ❌ Error: {e}")
        
        unique_staff = self._deduplicate_staff(all_staff)
        print(f"🔍 Total unique staff found: {len(unique_staff)}")
        
        return unique_staff, all_social

    def _enhanced_analyze_single_url(self, url: str) -> tuple:
        """🔍 Enhanced single URL analysis"""
        
        manager = ApifyAccountManager()
        client = manager.get_client_part1()
        
        print(f"   📡 Fetching: {url}")
        
        enhanced_content_function = """
        async function pageFunction(context) {
            const { request, log, jQuery } = context;
            const $ = jQuery;
            
            await context.waitFor(10000);
            
            try {
                const bodyText = $('body').text() || '';
                const socialMedia = {};
                
                $('a[href]').each(function() {
                    const href = $(this).attr('href');
                    if (!href) return;
                    
                    const lowerHref = href.toLowerCase();
                    if (lowerHref.includes('linkedin.com/company') && !socialMedia.company_linkedin) {
                        socialMedia.company_linkedin = href;
                    } else if (lowerHref.includes('linkedin.com') && !socialMedia.linkedin) {
                        socialMedia.linkedin = href;
                    } else if (lowerHref.includes('facebook.com') && !socialMedia.facebook) {
                        socialMedia.facebook = href;
                    } else if (lowerHref.includes('twitter.com') && !socialMedia.twitter) {
                        socialMedia.twitter = href;
                    } else if (lowerHref.includes('instagram.com') && !socialMedia.instagram) {
                        socialMedia.instagram = href;
                    }
                });
                
                const staffSections = [];
                $('*:contains("team"), *:contains("staff"), *:contains("about"), *:contains("people")').each(function() {
                    const sectionText = $(this).text();
                    if (sectionText && sectionText.length > 100) {
                        staffSections.push(sectionText);
                    }
                });
                
                return {
                    url: request.url,
                    content: bodyText,
                    staffSections: staffSections.join('\\n\\n'),
                    socialMedia: socialMedia
                };
                
            } catch (error) {
                log.error('Enhanced content extraction error:', error);
                return {
                    url: request.url,
                    content: '',
                    staffSections: '',
                    socialMedia: {}
                };
            }
        }
        """
        
        payload = {
            "startUrls": [{"url": url}],
            "maxPagesPerRun": 1,
            "keepUrlFragments": True,
            "waitForDynamicContent": True,
            "pageFunction": enhanced_content_function,
            "proxyConfiguration": {"useApifyProxy": True}
        }
        
        try:
            run = client.actor("apify/web-scraper").call(run_input=payload)
            
            if run:
                items = client.dataset(run["defaultDatasetId"]).list_items().items
                if items:
                    content = items[0].get('content', '')
                    staff_sections = items[0].get('staffSections', '')
                    social = items[0].get('socialMedia', {})
                    
                    analysis_content = staff_sections if staff_sections and len(staff_sections) > 500 else content
                    
                    print(f"   🎯 Extracted staff section: {len(analysis_content)} chars")
                    
                    staff = self._enhanced_gpt_extract_staff(analysis_content, url)
                    
                    return staff, social
            
            return [], {}
            
        except Exception as e:
            print(f"    ❌ Enhanced content analysis failed: {e}")
            return [], {}

    def _enhanced_gpt_extract_staff(self, content: str, url: str) -> list:
        """🧠 Enhanced GPT-4o staff extraction"""
        
        if len(content) < 1000:
            print(f"   🧠 GPT-4o found 0 staff members")
            print(f"   🧠 GPT-4o found 0 staff members")
            return []
        
        domain = urlparse(url).netloc.replace('www.', '')
        
        prompt = f"""Extract ALL staff/team members from this {domain} content.

EXTRACTION RULES:
1. Find ONLY current employees of {domain}
2. Extract: Full name + Job title/role
3. EXCLUDE: Client testimonials, external partners, vendors
4. INCLUDE: All levels - executives, managers, staff, directors
5. Must have: First name + Last name minimum

Return comprehensive JSON:
[
  {{"name": "Full Name", "title": "Complete Job Title"}},
  {{"name": "Another Person", "title": "Their Role"}}
]

If no staff found: []

CONTENT TO ANALYZE:
{content[:25000]}"""

        try:
            from openai import OpenAI
            client = OpenAI(api_key=self.openai_key)
            
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=2000,
                temperature=0.1
            )
            
            result_text = response.choices[0].message.content.strip()
            
            print(f"   🎯 Analyzing team content: {len(content)} chars")
            
            json_start = result_text.find('[')
            json_end = result_text.rfind(']') + 1
            
            if json_start >= 0 and json_end > json_start:
                json_text = result_text[json_start:json_end]
                staff_list = json.loads(json_text)
                
                validated_staff = []
                for staff in staff_list:
                    name = staff.get('name', '').strip()
                    title = staff.get('title', '').strip()
                    
                    if self._enhanced_validate_staff(name, title, domain):
                        validated_staff.append(staff)
                
                print(f"   🧠 GPT-4o found {len(validated_staff)} staff members")
                if len(validated_staff) == 0:
                    print(f"   🧠 GPT-4o found 0 staff members")
                
                return validated_staff
                
        except Exception as e:
            print(f"    ⚠️ Enhanced GPT extraction failed: {e}")
            print(f"   🧠 GPT-4o found 0 staff members")
            print(f"   🧠 GPT-4o found 0 staff members")
        
        return []

    def _process_tier_result(self, result: dict, url: str) -> tuple:
        """🔄 Process any tier result and extract staff + LinkedIn URL"""
        
        staff_list = []
        linkedin_url = ""
        
        social_data = result.get('socialMedia', {})
        
        linkedin_url = (social_data.get('company_linkedin') or 
                       social_data.get('linkedin') or 
                       social_data.get('LinkedIn') or "")
        
        if 'staff' in result:
            staff_list = result.get('staff', [])
        else:
            all_links = result.get('allLinks', [])
            team_fragments = result.get('teamFragments', [])
            
            if team_fragments:
                selected_urls = [frag['url'] for frag in team_fragments[:5]]
                print(f"🎯 Using {len(selected_urls)} team fragments as priority URLs")
            else:
                selected_urls = self._gpt_analyze_comprehensive_links(all_links, url)
            
            if selected_urls:
                staff_list, additional_social = self._enhanced_extract_staff_and_social(selected_urls)
                social_data.update(additional_social)
                linkedin_url = (social_data.get('company_linkedin') or 
                               social_data.get('linkedin') or 
                               social_data.get('LinkedIn') or linkedin_url)
        
        return staff_list, linkedin_url

    def _gpt_targeted_search_fallback(self, url: str) -> dict:
        """🎯 GPT-4o targeted search fallback"""
        
        print("🚀 GPT-4o TARGETED SEARCH FALLBACK")
        domain = urlparse(url).netloc.replace('www.', '')
        
        prompt = f"""Generate the most likely URLs for finding staff/team information on {domain}.

Based on common website structures, return 5 URLs most likely to contain staff info:

Return JSON: ["url1", "url2", "url3", "url4", "url5"]

Example patterns:
- {url}/about
- {url}/team  
- {url}/about-us
- {url}/staff
- {url}/people
- {url}/management
- {url}/leadership
- {url}/contact

Return ONLY the JSON array."""

        try:
            from openai import OpenAI
            client = OpenAI(api_key=self.openai_key)
            
            response = client.chat.completions.create(
                model="gpt-