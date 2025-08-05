# app/services/auth_service.py - Authentication service (fixed version)
import ssl
import hashlib
import requests
import urllib3
import re
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager
from urllib3.util.ssl_ import create_urllib3_context
from typing import Optional

from app.config.settings import settings

class SSLAdapter(HTTPAdapter):
    """Custom SSL adapter with proper poolmanager initialization"""
    
    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        """Initialize pool manager with custom SSL context"""
        # Create SSL context with relaxed security for compatibility
        ctx = create_urllib3_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        ctx.set_ciphers("DEFAULT@SECLEVEL=1")
        
        self.poolmanager = PoolManager(
            num_pools=connections,  # Fixed parameter name
            maxsize=maxsize,
            block=block,
            ssl_context=ctx,
            **pool_kwargs
        )

class AuthenticationService:
    """Handles authentication with proper error handling and session management"""
    
    def __init__(self):
        self.session: Optional[requests.Session] = None
        # Disable SSL warnings
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    def setup_authentication(self) -> requests.Session:
        """Main authentication method with session reuse"""
        if self.session and self._is_session_valid():
            print("♻️ Reusing existing valid session")
            return self.session
        
        print("🔐 Performing authentication...")
        self.session = self._perform_authentication()
        print("✅ Authentication successful!")
        return self.session
    
    def _perform_authentication(self) -> requests.Session:
        """Perform the authentication process with proper error handling"""
        try:
            # Validate required settings
            self._validate_settings()
            
            # Get MD5 password hash
            pw_md5 = hashlib.md5(settings.API_PASS.encode("utf-8")).hexdigest()
            
            # Create session with SSL adapter
            session = requests.Session()
            session.verify = False
            session.mount("https://", SSLAdapter())
            
            # Set default timeout for all requests
            session.timeout = getattr(settings, 'REQUEST_TIMEOUT', 30)
            
            # Initial page visit to establish session
            print("📄 Visiting login page...")
            initial_response = session.get(settings.LOGIN_PAGE)
            initial_response.raise_for_status()
            
            # Build login URL with timestamp
            timestamp = int(datetime.utcnow().timestamp() * 1000)
            login_url = f"{settings.LOGIN_URL}?_dc={timestamp}"
            
            # Login payload
            login_payload = {
                "phone": "",
                "messageCode": "",
                "userCode": settings.API_USER,
                "password": pw_md5,
                "orgUuid": settings.ORG_UUID
            }
            
            # Login headers
            login_headers = {
                "Content-Type": "application/json;charset=UTF-8",
                "Origin": settings.BASE_URL,
                "Referer": settings.LOGIN_PAGE,
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
            
            # Perform login
            print("🔑 Submitting login credentials...")
            timeout = getattr(settings, 'REQUEST_TIMEOUT', 30)
            if hasattr(settings, 'realtime_detector'):
                timeout = getattr(settings.realtime_detector, 'request_timeout', timeout)
            
            resp = session.post(
                login_url,
                json=login_payload,
                headers=login_headers,
                timeout=timeout
            )
            resp.raise_for_status()
            
            # Check for login success in response
            if resp.status_code != 200:
                raise RuntimeError(f"Login failed with status code: {resp.status_code}")
            
            # Debug: Print response details for troubleshooting
            print(f"🔍 Login response status: {resp.status_code}")
            print(f"🔍 Response headers: {dict(resp.headers)}")
            print(f"🔍 Response content preview: {resp.text[:200]}")
            
            # Extract JWT token from Set-Cookie header
            set_cookie = resp.headers.get("Set-Cookie", "")
            print(f"🔍 Set-Cookie header: {set_cookie}")
            
            jwt_match = re.search(r"jwt=([^;]+)", set_cookie)
            jwt_token = None
            
            if jwt_match:
                jwt_token = jwt_match.group(1)
                print(f"✅ JWT token found via regex: {jwt_token[:20]}...")
            else:
                # Try alternative cookie extraction methods
                print("🔍 Regex failed, checking session cookies...")
                for cookie in session.cookies:
                    print(f"🔍 Found cookie: {cookie.name} = {cookie.value[:20]}...")
                    if cookie.name.lower() == 'jwt':
                        jwt_token = cookie.value
                        print(f"✅ JWT token found in cookies: {jwt_token[:20]}...")
                        break
                
                # Check if JWT is in response body (some APIs return it there)
                if not jwt_token:
                    print("🔍 Checking response body for JWT...")
                    try:
                        response_data = resp.json()
                        print(f"🔍 Response JSON: {response_data}")
                        
                        # Common JWT response patterns
                        jwt_token = (response_data.get('token') or 
                                   response_data.get('jwt') or 
                                   response_data.get('access_token') or
                                   response_data.get('data', {}).get('token'))
                        
                        if jwt_token:
                            print(f"✅ JWT token found in response body: {jwt_token[:20]}...")
                    except Exception as e:
                        print(f"🔍 Could not parse response as JSON: {e}")
                
                if not jwt_token:
                    print("❌ No JWT token found anywhere!")
                    print(f"❌ Available cookies: {[c.name for c in session.cookies]}")
                    print(f"❌ Response headers: {list(resp.headers.keys())}")
                    raise RuntimeError(
                        f"Authentication failed: No JWT token found. "
                        f"Status: {resp.status_code}, "
                        f"Cookies: {[c.name for c in session.cookies]}, "
                        f"Headers: {list(resp.headers.keys())}"
                    )
            
            # Set JWT cookie with proper domain
            if jwt_token:
                domain = getattr(settings, 'COOKIE_DOMAIN', '.superbrandmall.com')
                session.cookies.set(
                    "jwt",
                    jwt_token,
                    domain=domain,
                    path="/"
                )
                print(f"🍪 JWT token set in session cookies for domain: {domain}")
            else:
                raise RuntimeError("No JWT token available to set in cookies")
            
            print("🍪 JWT token extracted and set successfully")
            return session
            
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Network error during authentication: {str(e)}")
        except Exception as e:
            raise RuntimeError(f"Authentication failed: {str(e)}")
    
    def _validate_settings(self):
        """Validate that all required settings are present"""
        required_settings = [
            'API_PASS', 'API_USER', 'ORG_UUID', 
            'LOGIN_PAGE', 'LOGIN_URL', 'BASE_URL'
        ]
        
        missing = []
        for setting in required_settings:
            if not hasattr(settings, setting) or not getattr(settings, setting):
                missing.append(setting)
        
        if missing:
            raise ValueError(f"Missing required settings: {', '.join(missing)}")
    
    def _is_session_valid(self) -> bool:
        """Check if current session is still valid"""
        if not self.session:
            return False
        
        try:
            # Test session with a lightweight request
            test_response = self.session.get(
                settings.LOGIN_PAGE, 
                timeout=5,
                allow_redirects=False
            )
            # If we get redirected to login, session is invalid
            if test_response.status_code in [302, 401, 403]:
                return False
            return test_response.status_code == 200
        except Exception:
            return False
    
    def get_session(self) -> requests.Session:
        """Get current authenticated session or create new one"""
        return self.setup_authentication()
    
    def invalidate_session(self):
        """Invalidate current session and clean up resources"""
        if self.session:
            try:
                self.session.close()
            except Exception:
                pass  # Ignore cleanup errors
        self.session = None
        print("🗑️ Session invalidated")
    
    def test_authentication(self) -> bool:
        """Test if authentication is working properly"""
        try:
            session = self.get_session()
            # You can add a specific test endpoint here
            test_response = session.get(settings.LOGIN_PAGE, timeout=10)
            return test_response.status_code == 200
        except Exception as e:
            print(f"❌ Authentication test failed: {str(e)}")
            return False

# Singleton instance for reuse across the application
auth_service = AuthenticationService()