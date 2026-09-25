import { createContext, useContext, useEffect, useState } from 'react';
import type { ReactNode } from 'react';
import type { User, UserManager } from 'oidc-client-ts';
import { api, configureAccessToken, message } from './api';
import type { AuthConfig } from './api';

interface AuthState { ready: boolean; authenticated: boolean; config?: AuthConfig; user?: User; error?: string; login: () => Promise<void>; logout: () => Promise<void>; retry: () => void }
const AuthContext = createContext<AuthState | undefined>(undefined);
let manager: UserManager | undefined;
let boot: Promise<{ config: AuthConfig; user?: User }> | undefined;

async function initialize() {
  const config = await api.authConfig();
  if (config.mode === 'development') { configureAccessToken(async () => undefined); return { config }; }
  if (config.mode !== 'oidc' || !config.issuer || !config.clientId) throw new Error('The administrator must finish the OIDC issuer and browser client configuration.');
  const { UserManager, WebStorageStateStore, InMemoryWebStorage } = await import('oidc-client-ts');
  manager = new UserManager({
    authority: config.issuer,
    client_id: config.clientId,
    redirect_uri: `${window.location.origin}/auth/callback`,
    post_logout_redirect_uri: window.location.origin,
    response_type: 'code',
    scope: Array.isArray(config.scopes) ? config.scopes.join(' ') : config.scopes || 'openid profile',
    // Only temporary PKCE interaction state survives a redirect. Tokens remain in memory.
    stateStore: new WebStorageStateStore({ store: window.sessionStorage, prefix: 'dataslate.oidc.state.' }),
    userStore: new WebStorageStateStore({ store: new InMemoryWebStorage() }),
    automaticSilentRenew: false,
    loadUserInfo: false,
    ...(config.audience ? { extraQueryParams: { audience: config.audience } } : {}),
  });
  configureAccessToken(async () => {
    const user = await manager?.getUser();
    if (user?.expired) { window.dispatchEvent(new Event('dataslate:unauthorized')); return undefined; }
    return user?.access_token;
  });
  let user: User | undefined;
  if (window.location.pathname === '/auth/callback') {
    try { user = await manager.signinRedirectCallback(); }
    catch { throw new Error('Sign-in could not be completed. Retry with your organization, or ask an administrator to check the browser client and API audience.'); }
    finally { window.history.replaceState(null, '', '/'); }
  }
  await manager.clearStaleState();
  return { config, user };
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState<Pick<AuthState, 'ready' | 'authenticated' | 'config' | 'user' | 'error'>>({ ready: false, authenticated: false });
  const [attempt, setAttempt] = useState(0);
  useEffect(() => {
    let mounted = true;
    boot ??= initialize();
    boot.then(({ config, user }) => { if (mounted) setState({ ready: true, authenticated: config.mode === 'development' || Boolean(user && !user.expired), config, user }); })
      .catch((error) => { if (mounted) setState({ ready: true, authenticated: false, error: message(error) }); });
    const expired = () => { if (mounted) setState((previous) => ({ ...previous, authenticated: false, error: 'Your session has expired. Sign in again to continue.' })); };
    window.addEventListener('dataslate:unauthorized', expired);
    return () => { mounted = false; window.removeEventListener('dataslate:unauthorized', expired); };
  }, [attempt]);
  const login = async () => {
    try { await manager?.signinRedirect(); }
    catch { setState((previous) => ({ ...previous, error: 'Sign-in could not start. Check the identity provider configuration and try again.' })); }
  };
  const logout = async () => {
    await manager?.removeUser();
    setState((previous) => ({ ...previous, authenticated: false, user: undefined }));
  };
  return <AuthContext.Provider value={{ ...state, login, logout, retry: () => { boot = undefined; setState({ ready: false, authenticated: false }); setAttempt((n) => n + 1); } }}>{children}</AuthContext.Provider>;
}

export function useAuth() { const auth = useContext(AuthContext); if (!auth) throw new Error('AuthProvider is required'); return auth; }
