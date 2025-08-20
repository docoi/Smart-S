"""
🌐 Enhanced Website Scraper Module - 3-TIER SYSTEM
=================================================
Handles website scraping with intelligent actor selection, staff extraction, and LinkedIn URL discovery
"""

import json
from urllib.parse import urlparse, urlunparse
from account_manager import get_working_apify_client_part1


class WebsiteScraper:
    """🌐 Enhanced website scraping with 3-tier intelligence and GPT-4o analysis"""
    
    def __init__(self, openai_key):
        self.openai_key = openai_key
    
    def scrape_website_for_staff_and_linkedin(self, website_url: str) -> tuple:
        """📋 MAIN METHOD: Enhanced website scraping with 3-tier actor selection"""
        
        print(f"🚀 ENHANCED WEBSITE MAPPING - 3-TIER SYSTEM")
        print(f"🌐 Target: {website_url}")
        print("=" * 60)
        
        # Normalize URL
        normalized_url = self._normalize_www(website_url)
        
        # PART 1A: Enhanced website mapping with 3-tier actor selection
        print(f"🗺️ PART 1A: ENHANCED WEBSITE MAPPING")
        print("-" * 40)
        website_map, social_links = self._enhanced_website_mapping(normalized_url)
        
        if not website_map:
            print("❌ Website mapping failed across all tiers")
            return [], ""
        
        # PART 1B: GPT-4o URL analysis and selection
        print(f"\n🧠 PART 1B: GPT-4O URL ANALYSIS")
        print("-" * 40)
        selected_urls = self._gpt_analyze_urls(website_map, normalized_url)
        
        if not selected_urls:
            print("❌ No URLs selected for analysis")
            return [], ""
        
        # PART 1C: Enhanced content scraping and staff extraction
        print(f"\n🔍 PART 1C: ENHANCED CONTENT ANALYSIS")
        print("-" * 40)
        staff_list, additional_social = self._enhanced_extract_staff_and_social(selected_urls)
        
        # Combine social media links from both phases
        all_social_data = {**social_links, **additional_social}
        
        # Find LinkedIn URL in correct format for LinkedIn scraper
        linkedin_url = self._extract_linkedin_url(all_social_data)
        
        print(f"\n✅ ENHANCED WEBSITE SCRAPING COMPLETE:")
        print(f"   👥 Staff found: {len(staff_list)}")
        print(f"   🔗 LinkedIn URL: {linkedin_url[:50] + '...' if linkedin_url and len(linkedin_url) > 50 else linkedin_url or 'None found'}")
        
        return staff_list, linkedin_url

    def _enhanced_website_mapping(self, url: str) -> tuple:
        """🎯 ENHANCED: 3-tier website mapping with intelligent actor selection"""
        
        print("🎯 3-TIER INTELLIGENT ACTOR SELECTION")
        print("🔄 Tier 1: Cheerio Scraper (HTML-optimized)")
        print("🔄 Tier 2: Web Scraper (JavaScript-heavy)")
        print("🔄 Tier 3: Fallback with enhanced config")
        print()
        
        # TIER 1: Cheerio Scraper (best for HTML-heavy sites)
        print("🔍 TIER 1: Testing Cheerio Scraper (HTML-optimized)...")
        website_map, social_links = self._tier1_cheerio_mapping(url)
        
        if website_map and len(website_map.get('allLinks', [])) > 5:
            print(f"✅ TIER 1 SUCCESS: Found {len(website_map.get('allLinks', []))} links")
            return website_map, social_links
        else:
            print("❌ TIER 1 FAILED: Insufficient data or JavaScript-heavy site detected")
        
        # TIER 2: Web Scraper (for JavaScript-heavy sites)
        print("\n🔍 TIER 2: Testing Web Scraper (JavaScript-heavy)...")
        website_map, social_links = self._tier2_web_scraper_mapping(url)
        
        if website_map and len(website_map.get('allLinks', [])) > 5:
            print(f"✅ TIER 2 SUCCESS: Found {len(website_map.get('allLinks', []))} links")
            return website_map, social_links
        else:
            print("❌ TIER 2 FAILED: Complex JavaScript or accessibility issues")
        
        # TIER 3: Enhanced fallback with different configuration
        print("\n🔍 TIER 3: Enhanced fallback configuration...")
        website_map, social_links = self._tier3_fallback_mapping(url)
        
        if website_map and len(website_map.get('allLinks', [])) > 0:
            print(f"✅ TIER 3 SUCCESS: Found {len(website_map.get('allLinks', []))} links")
            return website_map, social_links
        else:
            print("❌ ALL TIERS FAILED: Cannot map website")
            return {}, {}

    def _tier1_cheerio_mapping(self, url: str) -> tuple:
        """🥇 TIER 1: Cheerio Scraper - Optimized for HTML-heavy sites"""
        
        try:
            client = get_working_apify_client_part1()
            
            cheerio_input = {
                "startUrls": [{"url": url}],
                "maxPagesPerRun": 1,
                "pageLoadTimeoutSecs": 60,
                "useApifyProxy": True,
                "pageFunction": "async function pageFunction(context) { const { request, log, $ } = context; try { await context.waitFor(3000); const allLinks = []; const socialMedia = {}; const baseUrl = request.url.split('/').slice(0, 3).join('/'); $('a[href]').each(function() { const href = $(this).attr('href'); const text = $(this).text().trim(); if (href && href.length > 1) { let fullUrl = href; let isInternal = false; if (href.startsWith('/')) { fullUrl = baseUrl + href; isInternal = true; } else if (href.startsWith(baseUrl)) { isInternal = true; } else if (!href.startsWith('http')) { const basePath = request.url.split('/').slice(0, -1).join('/'); fullUrl = basePath + '/' + href; isInternal = true; } const lowerHref = href.toLowerCase(); if (lowerHref.includes('linkedin.com')) { socialMedia.linkedin = fullUrl; } else if (lowerHref.includes('facebook.com')) { socialMedia.facebook = fullUrl; } else if (lowerHref.includes('twitter.com')) { socialMedia.twitter = fullUrl; } else if (lowerHref.includes('instagram.com')) { socialMedia.instagram = fullUrl; } if (isInternal && !href.startsWith('mailto:') && !href.startsWith('tel:') && !href.startsWith('#') && !lowerHref.includes('linkedin.com') && !lowerHref.includes('facebook.com') && !lowerHref.includes('twitter.com') && !lowerHref.includes('instagram.com')) { allLinks.push({ url: fullUrl, text: text, href: href }); } } }); return { url: request.url, websiteMap: { allLinks: allLinks, domain: request.url.split('/')[2], tier: 'cheerio' }, socialMedia: socialMedia }; } catch (error) { log.error('Cheerio mapping error:', error); return { url: request.url, websiteMap: { allLinks: [], domain: request.url.split('/')[2], tier: 'cheerio' }, socialMedia: {} }; } }"
            }
            
            print(f"   🚀 Running Cheerio Scraper: {url}")
            run = client.actor("YrQuEkowkNCLdk4j2").call(run_input=cheerio_input)
            
            if run:
                items = client.dataset(run["defaultDatasetId"]).list_items().items
                if items and len(items) > 0:
                    result = items[0]
                    website_map = result.get('websiteMap', {})
                    social_media = result.get('socialMedia', {})
                    
                    if len(website_map.get('allLinks', [])) > 3:  # Lowered threshold
                        print(f"   ✅ Cheerio: {len(website_map.get('allLinks', []))} links found")
                        return website_map, social_media
            
            print(f"   ❌ Cheerio: Insufficient data returned")
            return {}, {}
            
        except Exception as e:
            print(f"   ❌ Cheerio Scraper failed: {e}")
            return {}, {}

    def _tier2_web_scraper_mapping(self, url: str) -> tuple:
        """🥈 TIER 2: Web Scraper - For JavaScript-heavy sites"""
        
        try:
            client = get_working_apify_client_part1()
            
            web_scraper_input = {
                "startUrls": [{"url": url}],
                "maxPagesPerRun": 1,
                "pageLoadTimeoutSecs": 90,
                "ignoreSslErrors": True,
                "useChrome": True,
                "useStealth": True,
                "proxyConfiguration": {"useApifyProxy": True},
                "pageFunction": "async function pageFunction(context) { const { request, log, jQuery } = context; const $ = jQuery; try { await context.waitFor(8000); await new Promise(resolve => setTimeout(resolve, 3000)); const allLinks = []; const socialMedia = {}; const baseUrl = request.url.split('/').slice(0, 3).join('/'); $('a[href]').each(function() { const href = $(this).attr('href'); const text = $(this).text().trim(); if (href && href.length > 1) { let fullUrl = href; let isInternal = false; if (href.startsWith('/')) { fullUrl = baseUrl + href; isInternal = true; } else if (href.startsWith(baseUrl)) { isInternal = true; } else if (!href.startsWith('http')) { const basePath = request.url.split('/').slice(0, -1).join('/'); fullUrl = basePath + '/' + href; isInternal = true; } const lowerHref = href.toLowerCase(); if (lowerHref.includes('linkedin.com')) { socialMedia.linkedin = fullUrl; } else if (lowerHref.includes('facebook.com')) { socialMedia.facebook = fullUrl; } else if (lowerHref.includes('twitter.com')) { socialMedia.twitter = fullUrl; } else if (lowerHref.includes('instagram.com')) { socialMedia.instagram = fullUrl; } if (isInternal && !href.startsWith('mailto:') && !href.startsWith('tel:') && !href.startsWith('#') && !lowerHref.includes('linkedin.com') && !lowerHref.includes('facebook.com') && !lowerHref.includes('twitter.com') && !lowerHref.includes('instagram.com')) { allLinks.push({ url: fullUrl, text: text, href: href }); } } }); return { url: request.url, websiteMap: { allLinks: allLinks, domain: request.url.split('/')[2], tier: 'web-scraper' }, socialMedia: socialMedia }; } catch (error) { log.error('Web scraper mapping error:', error); return { url: request.url, websiteMap: { allLinks: [], domain: request.url.split('/')[2], tier: 'web-scraper' }, socialMedia: {} }; } }"
            }
            
            print(f"   🚀 Running Web Scraper: {url}")
            run = client.actor("apify~web-scraper").call(run_input=web_scraper_input)
            
            if run:
                items = client.dataset(run["defaultDatasetId"]).list_items().items
                if items and len(items) > 0:
                    result = items[0]
                    website_map = result.get('websiteMap', {})
                    social_media = result.get('socialMedia', {})
                    
                    if len(website_map.get('allLinks', [])) > 3:  # Lowered threshold
                        print(f"   ✅ Web Scraper: {len(website_map.get('allLinks', []))} links found")
                        return website_map, social_media
            
            print(f"   ❌ Web Scraper: Insufficient data returned")
            return {}, {}
            
        except Exception as e:
            print(f"   ❌ Web Scraper failed: {e}")
            return {}, {}

    def _tier3_fallback_mapping(self, url: str) -> tuple:
        """🥉 TIER 3: Enhanced fallback with aggressive configuration"""
        
        try:
            client = get_working_apify_client_part1()
            
            fallback_input = {
                "startUrls": [{"url": url}],
                "maxPagesPerRun": 1,
                "pageLoadTimeoutSecs": 120,
                "ignoreSslErrors": True,
                "useChrome": True,
                "proxyConfiguration": {"useApifyProxy": True},
                "pageFunction": "async function pageFunction(context) { const { request, log, jQuery } = context; const $ = jQuery; try { await context.waitFor(15000); const allLinks = []; const socialMedia = {}; const baseUrl = request.url.split('/').slice(0, 3).join('/'); const linkSelectors = ['a[href]', '[href]', 'link[href]']; linkSelectors.forEach(selector => { $(selector).each(function() { const href = $(this).attr('href'); const text = $(this).text().trim() || $(this).attr('title') || 'Link'; if (href && href.length > 1) { let fullUrl = href; let isInternal = false; if (href.startsWith('/')) { fullUrl = baseUrl + href; isInternal = true; } else if (href.startsWith(baseUrl)) { isInternal = true; } else if (!href.startsWith('http') && !href.includes('.')) { fullUrl = baseUrl + '/' + href; isInternal = true; } const lowerHref = href.toLowerCase(); if (lowerHref.includes('linkedin.com')) { socialMedia.linkedin = fullUrl; } else if (lowerHref.includes('facebook.com')) { socialMedia.facebook = fullUrl; } else if (lowerHref.includes('twitter.com')) { socialMedia.twitter = fullUrl; } else if (lowerHref.includes('instagram.com')) { socialMedia.instagram = fullUrl; } if (isInternal && !href.startsWith('mailto:') && !href.startsWith('tel:') && !href.startsWith('#') && !lowerHref.includes('linkedin.com') && !lowerHref.includes('facebook.com') && !lowerHref.includes('twitter.com') && !lowerHref.includes('instagram.com')) { allLinks.push({ url: fullUrl, text: text, href: href }); } } }); }); const uniqueLinks = []; const seenUrls = new Set(); allLinks.forEach(link => { if (!seenUrls.has(link.url)) { seenUrls.add(link.url); uniqueLinks.push(link); } }); return { url: request.url, websiteMap: { allLinks: uniqueLinks, domain: request.url.split('/')[2], tier: 'fallback' }, socialMedia: socialMedia }; } catch (error) { log.error('Fallback mapping error:', error); return { url: request.url, websiteMap: { allLinks: [], domain: request.url.split('/')[2], tier: 'fallback' }, socialMedia: {} }; } }"
            }
            
            print(f"   🚀 Running Fallback Scraper: {url}")
            run = client.actor("apify~web-scraper").call(run_input=fallback_input)
            
            if run:
                items = client.dataset(run["defaultDatasetId"]).list_items().items
                if items and len(items) > 0:
                    result = items[0]
                    website_map = result.get('websiteMap', {})
                    social_media = result.get('socialMedia', {})
                    
                    print(f"   ✅ Fallback: {len(website_map.get('allLinks', []))} links found")
                    return website_map, social_media
            
            print(f"   ❌ Fallback: No data returned")
            return {}, {}
            
        except Exception as e:
            print(f"   ❌ Fallback Scraper failed: {e}")
            return {}, {}

    def _gpt_analyze_urls(self, website_map: dict, domain: str) -> list:
        """🧠 ENHANCED: GPT-4o analyzes URLs to select best ones for staff extraction"""
        
        all_links = website_map.get('allLinks', [])
        tier_used = website_map.get('tier', 'unknown')
        
        print(f"🧠 GPT-4O URL ANALYSIS")
        print(f"   📊 Links to analyze: {len(all_links)}")
        print(f"   🎯 Tier used: {tier_used}")
        
        if not all_links:
            print("   ❌ No links to analyze")
            return []
        
        # Create enhanced link summary for GPT
        link_summary = []
        for i, link in enumerate(all_links[:50]):
            url = link.get('url', '')
            text = link.get('text', '')
            href = link.get('href', '')
            link_summary.append(f"{i+1}. {text} → {href}")
        
        prompt = f"""Analyze this website and select the 3 BEST URLs for finding staff/team information.

DOMAIN: {domain}
SCRAPING TIER: {tier_used}
TOTAL LINKS FOUND: {len(all_links)}

AVAILABLE LINKS:
{chr(10).join(link_summary)}

CRITICAL INSTRUCTIONS:
1. PRIORITIZE WEBSITE PAGES over social media links
2. Look for: /about, /team, /staff, /people, /contact, /leadership, /management, /company
3. AVOID social media links (LinkedIn, Facebook, Instagram, Twitter) UNLESS no website pages exist
4. If only social media links available, select them as fallback
5. Return exactly 3 URLs as JSON

SELECTION PRIORITY:
- HIGH: Website pages with staff/team keywords
- MEDIUM: General website pages (about, contact, company info)
- LOW: Social media pages (only if no website pages found)

REQUIRED JSON FORMAT:
{{
  "selected_urls": [
    "full_url_1",
    "full_url_2", 
    "full_url_3"
  ],
  "reasoning": "Brief explanation of selection criteria and why these URLs were chosen"
}}"""

        try:
            from openai import OpenAI
            client = OpenAI(api_key=self.openai_key)
            
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=800,
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
                reasoning = analysis.get('reasoning', 'No reasoning provided')
                
                print(f"   🎯 GPT-4o selected {len(urls)} URLs:")
                print(f"   💭 Reasoning: {reasoning}")
                for i, url in enumerate(urls, 1):
                    print(f"      {i}. {url}")
                
                return urls
                
        except Exception as e:
            print(f"   ⚠️ GPT analysis failed: {e}")
        
        # Enhanced fallback: intelligent keyword-based selection
        print("   🔧 Using enhanced fallback URL selection...")
        return self._intelligent_fallback_url_selection(all_links, domain)

    def _intelligent_fallback_url_selection(self, all_links: list, domain: str) -> list:
        """🔧 Enhanced fallback URL selection with intelligent scoring"""
        
        staff_keywords = {
            'about': 10, 'team': 15, 'staff': 15, 'people': 12, 'contact': 8,
            'leadership': 12, 'management': 10, 'directors': 10, 'employees': 12,
            'our-team': 15, 'meet-the-team': 15, 'who-we-are': 8, 'company': 6
        }
        
        scored_links = []
        
        for link in all_links:
            url = link.get('url', '').lower()
            text = link.get('text', '').lower()
            href = link.get('href', '').lower()
            
            score = 0
            
            # Score based on URL path
            for keyword, weight in staff_keywords.items():
                if keyword in href or keyword in url:
                    score += weight
            
            # Score based on link text
            for keyword, weight in staff_keywords.items():
                if keyword.replace('-', ' ') in text:
                    score += weight * 0.8
            
            # Bonus for exact matches
            if any(exact in href for exact in ['/team', '/about', '/staff', '/people']):
                score += 20
            
            # Penalty for non-relevant pages
            if any(avoid in href for avoid in ['blog', 'news', 'product', 'service', 'shop']):
                score -= 5
            
            if score > 0:
                scored_links.append({
                    'url': link.get('url', ''),
                    'score': score,
                    'text': link.get('text', ''),
                    'href': link.get('href', '')
                })
        
        # Sort by score and return top 3
        scored_links.sort(key=lambda x: x['score'], reverse=True)
        selected_urls = [link['url'] for link in scored_links[:3]]
        
        print(f"   🎯 Fallback selected {len(selected_urls)} URLs based on scoring:")
        for i, link in enumerate(scored_links[:3], 1):
            print(f"      {i}. {link['href']} (Score: {link['score']})")
        
        return selected_urls

    def _enhanced_extract_staff_and_social(self, urls: list) -> tuple:
        """🔍 ENHANCED: Extract staff and social media from selected URLs with 3-tier logic"""
        
        print(f"🔍 ENHANCED CONTENT EXTRACTION")
        print(f"   📄 Processing {len(urls)} selected URLs")
        
        all_staff = []
        all_social = {}
        
        for i, url in enumerate(urls, 1):
            print(f"\n   📄 Processing URL {i}/{len(urls)}: {url}")
            
            try:
                staff, social = self._enhanced_analyze_single_url(url)
                
                if staff:
                    all_staff.extend(staff)
                    print(f"      ✅ Found {len(staff)} staff members")
                else:
                    print(f"      ❌ No staff found")
                
                if social:
                    all_social.update(social)
                    print(f"      🔗 Social media links: {len(social)}")
                
            except Exception as e:
                print(f"      ⚠️ Error processing URL: {e}")
                
        print(f"\n   📊 EXTRACTION SUMMARY:")
        print(f"      👥 Total staff found: {len(all_staff)}")
        print(f"      🔗 Social links found: {len(all_social)}")
        
        return all_staff, all_social

    def _enhanced_analyze_single_url(self, url: str) -> tuple:
        """🔍 ENHANCED: Analyze single URL using 3-tier logic"""
        
        # Try Tier 1 first (Cheerio)
        try:
            staff, social = self._tier1_content_extraction(url)
            if staff or social:
                print(f"      ✅ Tier 1 (Cheerio) successful")
                return staff, social
        except Exception as e:
            print(f"      ❌ Tier 1 failed: {e}")
        
        # Try Tier 2 (Web Scraper)
        try:
            staff, social = self._tier2_content_extraction(url)
            if staff or social:
                print(f"      ✅ Tier 2 (Web Scraper) successful")
                return staff, social
        except Exception as e:
            print(f"      ❌ Tier 2 failed: {e}")
        
        # Try Tier 3 (Fallback)
        try:
            staff, social = self._tier3_content_extraction(url)
            print(f"      ✅ Tier 3 (Fallback) completed")
            return staff, social
        except Exception as e:
            print(f"      ❌ All tiers failed: {e}")
            return [], {}

    def _tier1_content_extraction(self, url: str) -> tuple:
        """🥇 Tier 1: Cheerio content extraction"""
        
        try:
            client = get_working_apify_client_part1()
            
            cheerio_input = {
                "startUrls": [{"url": url}],
                "maxPagesPerRun": 1,
                "pageLoadTimeoutSecs": 60,
                "useApifyProxy": True,
                "pageFunction": "async function pageFunction(context) { const { request, log, $ } = context; try { await context.waitFor(5000); const bodyText = $('body').text() || ''; const socialMedia = {}; $('a[href]').each(function() { const href = $(this).attr('href'); if (!href) return; const lowerHref = href.toLowerCase(); if (lowerHref.includes('linkedin.com') && !socialMedia.linkedin) { socialMedia.linkedin = href; } else if (lowerHref.includes('facebook.com') && !socialMedia.facebook) { socialMedia.facebook = href; } else if (lowerHref.includes('twitter.com') && !socialMedia.twitter) { socialMedia.twitter = href; } else if (lowerHref.includes('instagram.com') && !socialMedia.instagram) { socialMedia.instagram = href; } }); return { url: request.url, content: bodyText, socialMedia: socialMedia }; } catch (error) { return { url: request.url, content: '', socialMedia: {} }; } }"
            }
            
            run = client.actor("YrQuEkowkNCLdk4j2").call(run_input=cheerio_input)
            
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
            raise Exception(f"Cheerio extraction failed: {e}")

    def _tier2_content_extraction(self, url: str) -> tuple:
        """🥈 Tier 2: Web Scraper content extraction"""
        
        try:
            client = get_working_apify_client_part1()
            
            web_scraper_input = {
                "startUrls": [{"url": url}],
                "maxPagesPerRun": 1,
                "pageLoadTimeoutSecs": 90,
                "useChrome": True,
                "useStealth": True,
                "proxyConfiguration": {"useApifyProxy": True},
                "pageFunction": "async function pageFunction(context) { const { request, log, jQuery } = context; const $ = jQuery; try { await context.waitFor(8000); const bodyText = $('body').text() || ''; const socialMedia = {}; $('a[href]').each(function() { const href = $(this).attr('href'); if (!href) return; const lowerHref = href.toLowerCase(); if (lowerHref.includes('linkedin.com') && !socialMedia.linkedin) { socialMedia.linkedin = href; } else if (lowerHref.includes('facebook.com') && !socialMedia.facebook) { socialMedia.facebook = href; } else if (lowerHref.includes('twitter.com') && !socialMedia.twitter) { socialMedia.twitter = href; } else if (lowerHref.includes('instagram.com') && !socialMedia.instagram) { socialMedia.instagram = href; } }); return { url: request.url, content: bodyText, socialMedia: socialMedia }; } catch (error) { return { url: request.url, content: '', socialMedia: {} }; } }"
            }
            
            run = client.actor("apify~web-scraper").call(run_input=web_scraper_input)
            
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
            raise Exception(f"Web Scraper extraction failed: {e}")

    def _tier3_content_extraction(self, url: str) -> tuple:
        """🥉 Tier 3: Fallback content extraction"""
        
        try:
            client = get_working_apify_client_part1()
            
            fallback_input = {
                "startUrls": [{"url": url}],
                "maxPagesPerRun": 1,
                "pageLoadTimeoutSecs": 120,
                "ignoreSslErrors": True,
                "useChrome": True,
                "proxyConfiguration": {"useApifyProxy": True},
                "pageFunction": "async function pageFunction(context) { const { request, log, jQuery } = context; const $ = jQuery; try { await context.waitFor(10000); const bodyText = $('body').text() || $('html').text() || ''; const socialMedia = {}; const selectors = ['a[href*=\"linkedin\"]', 'a[href*=\"facebook\"]', 'a[href*=\"twitter\"]', 'a[href*=\"instagram\"]']; selectors.forEach(selector => { $(selector).each(function() { const href = $(this).attr('href'); if (href) { const lowerHref = href.toLowerCase(); if (lowerHref.includes('linkedin.com') && !socialMedia.linkedin) { socialMedia.linkedin = href; } else if (lowerHref.includes('facebook.com') && !socialMedia.facebook) { socialMedia.facebook = href; } else if (lowerHref.includes('twitter.com') && !socialMedia.twitter) { socialMedia.twitter = href; } else if (lowerHref.includes('instagram.com') && !socialMedia.instagram) { socialMedia.instagram = href; } } }); }); return { url: request.url, content: bodyText, socialMedia: socialMedia }; } catch (error) { return { url: request.url, content: '', socialMedia: {} }; } }"
            }
            
            run = client.actor("apify~web-scraper").call(run_input=fallback_input)
            
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
            raise Exception(f"Fallback extraction failed: {e}")

    def _gpt_extract_staff(self, content: str, url: str) -> list:
        """🧠 ENHANCED: GPT-4o extracts staff from content with advanced analysis"""
        
        # Enhanced content threshold
        if len(content) < 1000:
            print(f"      ⚠️ Content too short ({len(content)} chars) - skipping GPT analysis")
            return []
        
        domain = urlparse(url).netloc.replace('www.', '')
        
        # Extract staff-focused sections
        staff_content = self._extract_staff_sections(content)
        if staff_content and len(staff_content) > len(content) * 0.1:
            content = staff_content
            print(f"      🎯 Using staff-focused content ({len(content)} chars)")
        
        prompt = f"""Extract staff information from this {domain} website content.

STRICT RULES:
1. Find ONLY current employees of {domain}
2. Each person needs: Full name (first + last) + Job title
3. EXCLUDE: Clients, testimonials, external people, quotes from non-employees
4. EXCLUDE: Company names that might look like person names
5. EXCLUDE: Single names without surnames
6. INCLUDE: Current team members, staff, employees, management, directors

Return as JSON array: [{{"name": "Full Name", "title": "Job Title"}}]
If no staff found: []

CONTENT TO ANALYZE:
{content[:100000]}"""

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
            
            print(f"      🧠 GPT-4o analysis complete ({len(result_text)} chars)")
            
            # Parse JSON response
            json_start = result_text.find('[')
            json_end = result_text.rfind(']') + 1
            
            if json_start >= 0 and json_end > json_start:
                json_text = result_text[json_start:json_end]
                staff_list = json.loads(json_text)
                
                # Enhanced staff validation
                validated_staff = []
                for staff in staff_list:
                    name = staff.get('name', '').strip()
                    title = staff.get('title', '').strip()
                    
                    if self._validate_staff_member(name, title, domain):
                        validated_staff.append({'name': name, 'title': title})
                        print(f"      ✅ Valid: {name} - {title}")
                    else:
                        print(f"      ❌ Invalid: {name} - {title}")
                
                return validated_staff[:15]  # Limit to top 15 results
                
        except json.JSONDecodeError as e:
            print(f"      ⚠️ JSON parsing failed: {e}")
        except Exception as e:
            print(f"      ⚠️ GPT extraction failed: {e}")
        
        return []

    def _extract_staff_sections(self, content: str) -> str:
        """🎯 Extract staff-related sections from content"""
        
        staff_keywords = [
            'about us', 'our team', 'meet the team', 'team members', 'staff',
            'our people', 'leadership', 'management', 'directors', 'employees',
            'who we are', 'team', 'about', 'leadership team', 'management team'
        ]
        
        lines = content.split('\n')
        staff_lines = []
        in_staff_section = False
        section_line_count = 0
        
        for line in lines:
            line_clean = line.lower().strip()
            
            # Check if we're entering a staff section
            if any(keyword in line_clean for keyword in staff_keywords):
                in_staff_section = True
                section_line_count = 0
                staff_lines.append(line)
                continue
            
            # If in staff section, collect lines
            if in_staff_section:
                staff_lines.append(line)
                section_line_count += 1
                
                # Stop after collecting substantial content or hitting clear break
                if (section_line_count > 100 or 
                    (len(line.strip()) == 0 and section_line_count > 30)):
                    break
        
        staff_content = '\n'.join(staff_lines)
        
        # Only return if we found substantial staff content
        if len(staff_content) > 500:
            return staff_content
        
        return content

    def _validate_staff_member(self, name: str, title: str, domain: str) -> bool:
        """🔍 Enhanced staff member validation"""
        
        if not name or not title or len(name.strip()) < 3:
            return False
        
        name = name.strip()
        title = title.strip()
        
        # Must have at least first and last name
        name_parts = name.split()
        if len(name_parts) < 2:
            return False
        
        # Check against company name variations
        company_variants = [
            domain.replace('.com', '').replace('.co.uk', '').replace('.ie', ''),
            domain.split('.')[0]
        ]
        
        for variant in company_variants:
            if name.lower().replace(' ', '') == variant.lower().replace(' ', ''):
                return False
        
        # Reject obvious non-person names
        reject_keywords = [
            'company', 'ltd', 'limited', 'inc', 'corp', 'llc', 'team', 
            'department', 'group', 'division', 'office', 'center', 'centre',
            'marketing', 'sales', 'support', 'admin', 'customer service'
        ]
        
        name_lower = name.lower()
        title_lower = title.lower()
        
        if any(keyword in name_lower for keyword in reject_keywords):
            return False
        
        # Reject generic titles that might not be real people
        generic_titles = ['company', 'business', 'team', 'department', 'office']
        if any(generic in title_lower for generic in generic_titles):
            return False
        
        # Validate name format (letters, spaces, hyphens, apostrophes only)
        import re
        if not re.match(r"^[a-zA-Z\s\-'\.]+$", name):
            return False
        
        return True

    def _extract_linkedin_url(self, social_data: dict) -> str:
        """🔗 Extract LinkedIn URL in format expected by LinkedIn scraper"""
        
        # Try different keys for LinkedIn URL
        linkedin_keys = ['linkedin', 'LinkedIn', 'company_linkedin', 'linkedIn']
        
        for key in linkedin_keys:
            url = social_data.get(key, '')
            if url:
                # Clean and validate LinkedIn URL
                cleaned_url = self._clean_linkedin_url(url)
                if cleaned_url:
                    print(f"   🔗 LinkedIn URL found: {cleaned_url}")
                    return cleaned_url
        
        print("   ❌ No valid LinkedIn URL found")
        return ""

    def _clean_linkedin_url(self, url: str) -> str:
        """🧹 Clean and validate LinkedIn URL"""
        
        if not url:
            return ""
        
        url = url.strip()
        
        # Ensure it's a LinkedIn company URL
        if 'linkedin.com' not in url.lower():
            return ""
        
        # Handle relative URLs
        if url.startswith('/'):
            url = 'https://www.linkedin.com' + url
        elif not url.startswith('http'):
            url = 'https://' + url
        
        # Ensure it's a company page, not a personal profile
        if '/company/' in url or '/companies/' in url:
            return url
        
        # Sometimes company URLs are in different formats
        if '/in/' not in url and 'linkedin.com' in url:
            return url
        
        return ""

    def _normalize_www(self, url: str) -> str:
        """🌐 Normalize URL format"""
        
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        parsed = urlparse(url)
        netloc = parsed.netloc
        
        # Add www if not present (some sites require it)
        if netloc and not netloc.startswith('www.'):
            netloc = 'www.' + netloc
        
        return urlunparse((
            parsed.scheme, netloc, parsed.path, 
            parsed.params, parsed.query, parsed.fragment
        ))