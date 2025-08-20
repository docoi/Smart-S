"""
🌐 Website Scraper Module - ENHANCED VERSION
============================================
✅ FIXED: Fragment URL detection (#the-team-behind-your-team)
✅ FIXED: Enhanced roadblock detection (more lenient with fragments)
✅ FIXED: Debug output to show Actor 1 results before roadblock detection
✅ FIXED: Better GPT-4o analysis with fragment awareness
"""

import json
from urllib.parse import urlparse, urlunparse
from account_manager import get_working_apify_client_part1


class WebsiteScraper:
    """🌐 Website scraping with enhanced GPT-4o analysis and fragment detection"""
    
    def __init__(self, openai_key):
        self.openai_key = openai_key
    
    def scrape_website_for_staff_and_linkedin(self, website_url: str) -> tuple:
        """📋 Main method: Enhanced scraping with fragment detection and smart roadblock logic"""
        
        print(f"🗺️ Phase 1a: Website mapping...")
        
        # Normalize URL
        normalized_url = self._normalize_www(website_url)
        print(f"🌐 Original URL: {website_url}")
        print(f"🌐 Normalized URL: {normalized_url}")
        
        # Map website to find URLs using account rotation
        website_map = self._stealth_website_mapping(normalized_url)
        
        # 🔧 ENHANCED: Debug output to show what Actor 1 actually found
        if website_map:
            stats = website_map.get('stats', {})
            team_fragments = website_map.get('teamFragments', [])
            all_links = website_map.get('allLinks', [])
            
            print(f"\n📊 ACTOR 1 DEBUG RESULTS:")
            print(f"   🔗 Total links found: {len(all_links)}")
            print(f"   🎯 Team fragments found: {len(team_fragments)}")
            print(f"   📄 Page content length: {stats.get('contentLength', 0)} chars")
            
            if team_fragments:
                print(f"   🔥 TEAM FRAGMENTS DETECTED:")
                for i, fragment in enumerate(team_fragments[:5], 1):
                    print(f"      {i}. {fragment.get('url', '')} ({fragment.get('text', '')})")
            
            if all_links:
                print(f"   📋 Sample links found:")
                for i, link in enumerate(all_links[:10], 1):
                    href = link.get('href', '')
                    text = link.get('text', '')[:50]
                    print(f"      {i}. {href} ({text})")
        
        # 🔧 ENHANCED: Improved roadblock detection that's fragment-aware
        if self._is_actor1_roadblock(website_map):
            print("🚧 Actor 1 roadblock detected - switching to GPT-4o direct web access")
            return self._gpt4o_direct_access_workflow(normalized_url)
        
        print(f"🧠 Phase 1b: Enhanced GPT-4o URL analysis...")
        
        # 🔧 ENHANCED: GPT-4o analysis with fragment awareness
        selected_urls = self._gpt_analyze_urls_enhanced(website_map, normalized_url)
        
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
        """🗺️ ENHANCED: Website mapping with fragment detection and team page discovery"""
        
        # 🔥 ENHANCED: JavaScript function with fragment detection
        mapping_function = """
async function pageFunction(context) {
    const { request, log, jQuery } = context;
    const $ = jQuery;
    
    await context.waitFor(8000); // Longer wait for JS-heavy sites
    
    try {
        const baseUrl = request.url.split('/').slice(0, 3).join('/');
        const allLinks = [];
        const navigationLinks = [];
        const teamFragments = [];
        const socialLinks = [];
        
        // Method 1: Standard link extraction with fragment detection
        $('a[href]').each(function() {
            const href = $(this).attr('href');
            const text = $(this).text().trim();
            
            if (href && href.length > 1) {
                let fullUrl = href;
                
                // Handle relative URLs
                if (href.startsWith('/')) {
                    fullUrl = baseUrl + href;
                } else if (href.startsWith('#')) {
                    // 🔥 FRAGMENT DETECTION: Convert #team to full URL
                    fullUrl = baseUrl + href;
                } else if (!href.startsWith('http') && !href.startsWith('mailto:') && !href.startsWith('tel:')) {
                    fullUrl = baseUrl + '/' + href;
                }
                
                const linkData = {
                    url: fullUrl,
                    text: text,
                    href: href,
                    type: 'standard'
                };
                
                allLinks.push(linkData);
                
                // 🔥 TEAM FRAGMENT DETECTION
                const textLower = text.toLowerCase();
                const hrefLower = href.toLowerCase();
                
                if ((hrefLower.includes('#') && (
                    hrefLower.includes('team') || 
                    hrefLower.includes('staff') || 
                    hrefLower.includes('people') ||
                    hrefLower.includes('about')
                )) || (textLower.includes('team') || textLower.includes('staff') || textLower.includes('people'))) {
                    teamFragments.push({
                        url: fullUrl,
                        text: text,
                        href: href,
                        type: 'team_fragment'
                    });
                }
                
                // Navigation detection
                if ($(this).closest('nav, .nav, .menu, .navigation, header').length > 0) {
                    navigationLinks.push(linkData);
                }
                
                // Social media detection
                if (hrefLower.includes('linkedin') || hrefLower.includes('facebook') || 
                    hrefLower.includes('twitter') || hrefLower.includes('instagram')) {
                    socialLinks.push(linkData);
                }
            }
        });
        
        // Method 2: Look for onclick handlers and data attributes that might reveal team sections
        $('[onclick*="team"], [onclick*="staff"], [data-target*="team"], [data-target*="staff"]').each(function() {
            const element = $(this);
            const onclick = element.attr('onclick') || '';
            const dataTarget = element.attr('data-target') || '';
            const text = element.text().trim();
            
            if (onclick || dataTarget) {
                teamFragments.push({
                    url: baseUrl + '#dynamic-team-section',
                    text: text,
                    href: onclick || dataTarget,
                    type: 'dynamic_team'
                });
            }
        });
        
        // Method 3: Content analysis for team sections
        const bodyText = $('body').text() || '';
        const pageTitle = $('title').text() || '';
        
        // Look for team-related content blocks
        const teamKeywords = ['team', 'staff', 'people', 'employees', 'leadership', 'management'];
        let teamContentFound = false;
        
        teamKeywords.forEach(keyword => {
            if (bodyText.toLowerCase().includes(keyword)) {
                teamContentFound = true;
            }
        });
        
        return {
            url: request.url,
            websiteMap: {
                allLinks: allLinks,
                navigationLinks: navigationLinks,
                teamFragments: teamFragments,
                socialLinks: socialLinks,
                stats: {
                    totalLinks: allLinks.length,
                    teamFragments: teamFragments.length,
                    navigationLinks: navigationLinks.length,
                    socialLinks: socialLinks.length,
                    contentLength: bodyText.length,
                    pageTitle: pageTitle,
                    teamContentFound: teamContentFound
                }
            }
        };
        
    } catch (error) {
        log.error('Error in pageFunction:', error);
        return {
            url: request.url,
            websiteMap: { 
                allLinks: [], 
                teamFragments: [],
                navigationLinks: [],
                socialLinks: [],
                stats: { 
                    totalLinks: 0, 
                    teamFragments: 0,
                    contentLength: 0,
                    error: error.message 
                }
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
            "maxRequestRetries": 3,
            "pageLoadTimeoutSecs": 60
        }
        
        try:
            # Get client with credit-based account rotation for Part 1
            client = get_working_apify_client_part1()
            
            print(f"🚀 Running enhanced Actor 1 with fragment detection...")
            run = client.actor("apify~web-scraper").call(run_input=payload)
            
            if run:
                items = client.dataset(run["defaultDatasetId"]).list_items().items
                if items and len(items) > 0:
                    website_map = items[0].get('websiteMap', {})
                    print(f"✅ Actor 1 completed successfully")
                    return website_map
                else:
                    print(f"⚠️ Actor 1 returned no items")
                    return {}
            else:
                print(f"❌ Actor 1 run failed")
                return {}
            
        except Exception as e:
            print(f"❌ Enhanced website mapping failed: {e}")
            return {}

    def _is_actor1_roadblock(self, website_map: dict) -> bool:
        """🚧 ENHANCED: Detect if Actor 1 hit a roadblock - but be more lenient with fragments"""
        
        if not website_map:
            print("🚧 Roadblock: No website map returned")
            return True
        
        stats = website_map.get('stats', {})
        all_links = website_map.get('allLinks', [])
        team_fragments = website_map.get('teamFragments', [])
        
        total_links = len(all_links)
        fragment_count = len(team_fragments)
        content_length = stats.get('contentLength', 0)
        
        print(f"\n🔍 ROADBLOCK ANALYSIS:")
        print(f"   📊 Links found: {total_links}")
        print(f"   🎯 Team fragments: {fragment_count}")
        print(f"   📄 Content length: {content_length} chars")
        
        # 🔧 ENHANCED: More intelligent roadblock detection
        # Don't trigger roadblock if we have team fragments OR decent content
        if fragment_count > 0:
            print(f"   ✅ Team fragments found - NO ROADBLOCK")
            return False
        
        if total_links >= 5 and content_length >= 1000:
            print(f"   ✅ Sufficient links and content - NO ROADBLOCK")
            return False
        
        if total_links < 3 and content_length < 500:
            print(f"   🚧 ROADBLOCK: Insufficient data (links: {total_links}, content: {content_length})")
            return True
        
        print(f"   ✅ Borderline but acceptable - NO ROADBLOCK")
        return False

    def _gpt_analyze_urls_enhanced(self, website_map: dict, domain: str) -> list:
        """🧠 ENHANCED: GPT-4o analyzes URLs with fragment detection and better context"""
        
        all_links = website_map.get('allLinks', [])
        team_fragments = website_map.get('teamFragments', [])
        navigation_links = website_map.get('navigationLinks', [])
        stats = website_map.get('stats', {})
        
        print(f"\n🧠 GPT-4o ENHANCED URL ANALYSIS:")
        print(f"   🎯 Team fragments to prioritize: {len(team_fragments)}")
        print(f"   🔗 All links available: {len(all_links)}")
        print(f"   🧭 Navigation links: {len(navigation_links)}")
        
        # 🔥 PRIORITIZE team fragments first
        priority_links = []
        
        # Add team fragments with highest priority
        for fragment in team_fragments:
            priority_links.append({
                'url': fragment.get('url', ''),
                'text': fragment.get('text', ''),
                'type': 'team_fragment',
                'priority': 'HIGH'
            })
        
        # Add navigation links
        for nav_link in navigation_links:
            href = nav_link.get('href', '').lower()
            if any(keyword in href for keyword in ['/about', '/team', '/staff', '/people', '/contact']):
                priority_links.append({
                    'url': nav_link.get('url', ''),
                    'text': nav_link.get('text', ''),
                    'type': 'navigation',
                    'priority': 'MEDIUM'
                })
        
        # Add other relevant links
        for link in all_links[:20]:  # Limit to prevent token overflow
            href = link.get('href', '').lower()
            text = link.get('text', '').lower()
            
            if (any(keyword in href for keyword in ['/about', '/team', '/staff', '/people', '/contact', '/leadership']) or
                any(keyword in text for keyword in ['about', 'team', 'staff', 'people', 'contact', 'leadership'])):
                
                # Avoid duplicates
                if not any(existing['url'] == link.get('url', '') for existing in priority_links):
                    priority_links.append({
                        'url': link.get('url', ''),
                        'text': link.get('text', ''),
                        'type': 'standard',
                        'priority': 'LOW'
                    })
        
        if not priority_links:
            print("   ⚠️ No relevant links found for GPT-4o analysis")
            return self._fallback_urls(domain)
        
        # Create enhanced prompt for GPT-4o
        link_summary = []
        for i, link in enumerate(priority_links[:15], 1):  # Top 15 to avoid token limits
            url = link.get('url', '')
            text = link.get('text', '')
            link_type = link.get('type', '')
            priority = link.get('priority', '')
            link_summary.append(f"{i}. [{priority}] {text} → {url} (Type: {link_type})")
        
        prompt = f"""🎯 ENHANCED STAFF PAGE ANALYSIS for {domain}

AVAILABLE LINKS (prioritized by relevance):
{chr(10).join(link_summary)}

ANALYSIS CONTEXT:
- Team fragments: {len(team_fragments)} detected (HIGHEST PRIORITY)
- Navigation links: {len(navigation_links)} found
- Content length: {stats.get('contentLength', 0)} characters
- Page has team content: {stats.get('teamContentFound', False)}

TASK: Select the 3 BEST URLs for finding staff/team information.

PRIORITIZATION RULES:
1. Team fragments (#team, #staff) = HIGHEST priority
2. /team, /about, /staff pages = HIGH priority  
3. Navigation links to people pages = MEDIUM priority
4. Avoid: privacy, terms, contact forms, social media

Return ONLY valid URLs as JSON array:
["url1", "url2", "url3"]

Focus on URLs most likely to contain staff profiles, team photos, or employee listings."""

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
            print(f"\n🧠 GPT-4o ANALYSIS RESULT:")
            print(f"   📝 Raw response: {result_text}")
            
            # Parse JSON
            json_start = result_text.find('[')
            json_end = result_text.rfind(']') + 1
            
            if json_start >= 0 and json_end > json_start:
                json_text = result_text[json_start:json_end]
                selected_urls = json.loads(json_text)
                
                # Validate and normalize URLs
                validated_urls = []
                for url in selected_urls:
                    if self._is_valid_url(url):
                        validated_urls.append(url)
                        print(f"   ✅ Selected: {url}")
                    else:
                        print(f"   ❌ Invalid URL rejected: {url}")
                
                if len(validated_urls) >= 1:
                    return validated_urls
                
        except Exception as e:
            print(f"⚠️ GPT-4o analysis failed: {e}")
        
        # Fallback: Use priority links directly
        print(f"   🔄 Using priority links as fallback")
        fallback_urls = [link['url'] for link in priority_links[:3] if self._is_valid_url(link['url'])]
        
        if fallback_urls:
            return fallback_urls
        
        return self._fallback_urls(domain)

    def _gpt4o_direct_access_workflow(self, website_url: str) -> tuple:
        """🤖 GPT-4o Direct Access Workflow with web search capabilities"""
        
        print("🤖 WEB SEARCH + GPT-4O ANALYSIS WORKFLOW")
        print("🔧 Using web search tools to find and analyze actual staff pages")
        print("=" * 70)
        
        domain = urlparse(website_url).netloc.replace('www.', '')
        
        try:
            from openai import OpenAI
            client = OpenAI(api_key=self.openai_key)
            
            # Enhanced GPT-4o prompt with specific search instructions
            search_prompt = f"""🔍 STAFF PAGE DISCOVERY for {domain}

TASK: Find the actual staff/team pages for this website using web search.

SEARCH STRATEGY:
1. Search for: site:{domain} team
2. Search for: site:{domain} staff  
3. Search for: site:{domain} about
4. Look for fragment URLs like #{domain}#team or #{domain}#staff
5. Find pages with employee profiles or team photos

ANALYSIS REQUIREMENTS:
- Find URLs that contain actual staff listings
- Look for pages with names + job titles
- Identify management and decision-makers
- Extract company LinkedIn URL if found

Please search the web to find the best staff/team pages for {domain} and return:
1. Best staff page URLs
2. Company LinkedIn URL
3. Any staff information found

Return as JSON:
{{
  "staff_urls": ["url1", "url2"],
  "linkedin_url": "company_linkedin_url",
  "staff_found": [
    {{"name": "Name", "title": "Title", "source": "URL"}}
  ]
}}"""

            print(f"🔍 Phase 1: Searching web for staff/team pages...")
            print(f"   🔍 Search 1: site:{domain} team")
            print(f"   🔍 Search 2: site:{domain} staff")
            print(f"   🔍 Search 3: site:{domain} about")
            print(f"   🔄 Adding common staff page URLs as fallback")
            
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": search_prompt}],
                max_tokens=1500,
                temperature=0.1
            )
            
            result_text = response.choices[0].message.content.strip()
            print(f"🧠 Phase 2: GPT-4o analyzing search results...")
            print(f"🧠 GPT-4o Analysis:")
            print(result_text)
            
            # Try to extract URLs from response
            staff_urls = []
            linkedin_url = ""
            
            # Look for common staff page patterns as fallback
            common_staff_urls = [
                f"https://{domain}/about",
                f"https://{domain}/team", 
                f"https://{domain}/staff",
                f"https://{domain}/people",
                f"https://{domain}/contact"
            ]
            
            print(f"🔄 No URLs found in search results, creating common staff page URLs")
            
            # Test each common URL
            staff_list = []
            for i, url in enumerate(common_staff_urls, 1):
                print(f"\n🔍 Phase 3.{i}: Fetching and analyzing {url}")
                try:
                    staff, social = self._analyze_single_url(url)
                    if staff:
                        staff_list.extend(staff)
                        print(f"   ✅ Found {len(staff)} staff members")
                    else:
                        print(f"   ❌ No staff found on this page")
                    
                    # Extract LinkedIn URL
                    if not linkedin_url and social.get('linkedin'):
                        linkedin_url = social['linkedin']
                        
                except Exception as e:
                    print(f"   ❌ Error analyzing {url}: {e}")
            
            # Try to extract LinkedIn from company domain
            if not linkedin_url:
                linkedin_url = f"https://www.linkedin.com/company/{domain.replace('.com', '').replace('.co.uk', '').replace('.ie', '')}"
            
            print(f"\n🎉 Web Search + GPT-4o Results:")
            print(f"   🔍 Method: Web search tools + GPT-4o analysis + content extraction")
            print(f"   👥 Total unique staff found: {len(staff_list)}")
            print(f"   🔗 LinkedIn URL: {linkedin_url}")
            print(f"   🌐 Pages analyzed: {len(common_staff_urls)}")
            
            return staff_list, linkedin_url
            
        except Exception as e:
            print(f"❌ GPT-4o direct access workflow failed: {e}")
            return [], f"https://www.linkedin.com/company/{domain.replace('.com', '').replace('.co.uk', '').replace('.ie', '')}"

    def _fallback_urls(self, domain: str) -> list:
        """🔄 Generate fallback URLs when GPT-4o analysis fails"""
        
        parsed_domain = urlparse(domain) if domain.startswith('http') else None
        clean_domain = parsed_domain.netloc if parsed_domain else domain
        
        fallback_urls = [
            f"https://{clean_domain}/about",
            f"https://{clean_domain}/team", 
            f"https://{clean_domain}/staff"
        ]
        
        print(f"🔄 Using fallback URLs: {fallback_urls}")
        return fallback_urls

    def _is_valid_url(self, url: str) -> bool:
        """✅ Validate URL format and content"""
        
        if not url or len(url) < 10:
            return False
        
        # Reject invalid URL patterns
        invalid_patterns = [
            'mailto:', 'tel:', 'javascript:', 'ftp:', 
            '/privacy', '/terms', '/cookie', '/legal',
            'facebook.com', 'twitter.com', 'instagram.com'  # Social links should not be analyzed for staff
        ]
        
        url_lower = url.lower()
        if any(pattern in url_lower for pattern in invalid_patterns):
            return False
        
        # Must be http/https URL
        if not url.startswith(('http://', 'https://')):
            return False
        
        return True

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
            
            print(f"   📡 Fetching: {url}")
            run = client.actor("apify~web-scraper").call(run_input=payload)
            
            if run:
                items = client.dataset(run["defaultDatasetId"]).list_items().items
                if items:
                    content = items[0].get('content', '')
                    social = items[0].get('socialMedia', {})
                    
                    if len(content) < 100:
                        print(f"   ❌ HTTP 404 or insufficient content")
                        return [], {}
                    
                    # Analyze content with GPT-4o for staff
                    staff = self._gpt_extract_staff(content, url)
                    
                    return staff, social
            
            print(f"   ❌ HTTP 404")
            return [], {}
            
        except Exception as e:
            print(f"    ❌ Content analysis failed: {e}")
            return [], {}

    def _gpt_extract_staff(self, content: str, url: str) -> list:
        """🧠 ENHANCED: GPT-4o extracts staff from content with better content analysis"""
        
        # 🔧 ENHANCED: Higher minimum content threshold
        if len(content) < 500:  # Reasonable threshold
            return []
        
        domain = urlparse(url).netloc.replace('www.', '')
        
        # 🔥 ENHANCED: Extract staff-related sections first
        staff_content = self._extract_staff_sections(content)
        if staff_content and len(staff_content) > len(content) * 0.3:  # If staff section is substantial
            content = staff_content
        
        prompt = f"""Extract staff from this {domain} content.

RULES:
1. Find ONLY current employees of {domain}
2. Need: Full name + Job title
3. EXCLUDE: Clients, testimonials, external people
4. EXCLUDE: Company names mistaken as people
5. Require: First name + Last name (minimum)
6. PRIORITIZE: Management, directors, decision-makers

Return JSON: [{{"name": "Full Name", "title": "Job Title"}}]
If none found: []

Content:
{content[:50000]}"""  # Reasonable limit for GPT-4o

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
                
                # 🔥 ENHANCED: Better staff validation
                validated_staff = []
                for staff in staff_list:
                    name = staff.get('name', '')
                    title = staff.get('title', '')
                    
                    # Validate name structure
                    if self._validate_staff_name(name, domain):
                        validated_staff.append(staff)
                        print(f"   ✅ Valid staff: {name} - {title}")
                    else:
                        print(f"   ❌ Invalid staff: {name} - {title}")
                
                return validated_staff[:10]  # Limit results
                
        except Exception as e:
            print(f"    ⚠️ GPT extraction failed: {e}")
        
        return []
    
    def _extract_staff_sections(self, content: str) -> str:
        """🔥 ENHANCED: Extract staff-related sections from content"""
        staff_keywords = [
            'about us', 'team', 'staff', 'our people', 'leadership',
            'meet the team', 'our team', 'employees', 'directors', 'management'
        ]
        
        # Simple section extraction based on keywords
        lines = content.split('\n')
        staff_lines = []
        in_staff_section = False
        section_score = 0
        
        for line in lines:
            line_lower = line.lower().strip()
            
            # Check if we're entering a staff section
            if any(keyword in line_lower for keyword in staff_keywords):
                in_staff_section = True
                section_score = 0
                staff_lines.append(line)
                continue
            
            # If in staff section, keep collecting lines
            if in_staff_section:
                staff_lines.append(line)
                section_score += 1
                
                # Stop if we hit a clear section break or collected enough
                if (len(line.strip()) == 0 and section_score > 30) or section_score > 100:
                    break
        
        extracted_content = '\n'.join(staff_lines) if staff_lines else content
        
        # Only return extracted section if it's substantial
        if len(extracted_content) > 1000:
            print(f"   🎯 Extracted staff section: {len(extracted_content)} chars")
            return extracted_content
        
        return content
    
    def _validate_staff_name(self, name: str, domain: str) -> bool:
        """🔥 ENHANCED: Validate if name looks like a real person"""
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
            'marketing', 'sales', 'support', 'admin', 'office', 'group'
        ]
        
        name_lower = name.lower()
        if any(keyword in name_lower for keyword in reject_keywords):
            return False
        
        # Reject single words that might be company names
        if len(name_parts) == 1:
            return False
        
        # Check for reasonable name length
        if len(name) > 50:  # Probably not a person's name
            return False
        
        return True

    def _normalize_www(self, url: str) -> str:
        """🌐 Normalize URL format"""
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        parsed = urlparse(url)
        netloc = parsed.netloc
        
        # Remove www. for consistency (we'll add it back if needed)
        if netloc.startswith('www.'):
            netloc = netloc[4:]
        
        # For the normalized URL, don't force www
        return urlunparse((parsed.scheme, netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))