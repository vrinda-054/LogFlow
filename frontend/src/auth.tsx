import { createContext, useContext, useEffect, useState, ReactNode } from 'react';
import { getHealth, HealthResponse } from './api';

export type User = {
  email: string;
  name: string;
  role: string;
};

export type AuthState = {
  user: User | null;
  isAuthenticated: boolean;
  token: string | null;
};

const AUTH_STORAGE_KEY = 'logflow_auth_session';
const REMEMBER_STORAGE_KEY = 'logflow_auth_remember';

export interface AuthContextType {
  user: User | null;
  token: string | null;
  isAuthenticated: boolean;
  isLoading: boolean;
  login: (email: string, pass: string, rememberMe: boolean) => Promise<void>;
  logout: () => void;
  systemHealth: HealthResponse | null;
  checkHealth: () => Promise<HealthResponse>;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [authState, setAuthState] = useState<AuthState>(() => {
    try {
      const saved = localStorage.getItem(AUTH_STORAGE_KEY) || sessionStorage.getItem(AUTH_STORAGE_KEY);
      if (saved) {
        const parsed = JSON.parse(saved);
        if (parsed?.token && parsed?.user) {
          return {
            user: parsed.user,
            token: parsed.token,
            isAuthenticated: true,
          };
        }
      }
    } catch {
      // Ignore storage parse errors
    }
    return {
      user: null,
      token: null,
      isAuthenticated: false,
    };
  });

  const [isLoading, setIsLoading] = useState<boolean>(false);
  const [systemHealth, setSystemHealth] = useState<HealthResponse | null>(null);

  const checkHealth = async (): Promise<HealthResponse> => {
    try {
      const health = await getHealth();
      setSystemHealth(health);
      return health;
    } catch (err) {
      const fallback: HealthResponse = {
        status: 'unreachable',
        database: 'unreachable',
        error: err instanceof Error ? err.message : 'Server connection failed',
      };
      setSystemHealth(fallback);
      return fallback;
    }
  };

  useEffect(() => {
    void checkHealth();
    const timer = setInterval(() => void checkHealth(), 15000);
    return () => clearInterval(timer);
  }, []);

  /**
   * Frontend Authentication Handler
   *
   * Note for Integration:
   * When the FastAPI backend implements a `/auth/login` endpoint:
   * Replace this simulation with:
   * const response = await fetch(`${API_BASE_URL}/auth/login`, {
   *   method: 'POST',
   *   headers: { 'Content-Type': 'application/json' },
   *   body: JSON.stringify({ email, password })
   * });
   */
  const login = async (email: string, _pass: string, rememberMe: boolean): Promise<void> => {
    setIsLoading(true);

    try {
      // Check server reachability
      const health = await checkHealth();

      // Simulate network request time for integration-ready frontend state
      await new Promise((resolve) => setTimeout(resolve, 500));

      const mockUser: User = {
        email: email.trim(),
        name: email.split('@')[0] || 'Operator',
        role: 'Logflow Operator',
      };
      const mockToken = `lf_token_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`;

      const dataToSave = JSON.stringify({ user: mockUser, token: mockToken });

      if (rememberMe) {
        localStorage.setItem(AUTH_STORAGE_KEY, dataToSave);
        localStorage.setItem(REMEMBER_STORAGE_KEY, 'true');
        sessionStorage.removeItem(AUTH_STORAGE_KEY);
      } else {
        sessionStorage.setItem(AUTH_STORAGE_KEY, dataToSave);
        localStorage.removeItem(AUTH_STORAGE_KEY);
        localStorage.removeItem(REMEMBER_STORAGE_KEY);
      }

      setAuthState({
        user: mockUser,
        token: mockToken,
        isAuthenticated: true,
      });

    } finally {
      setIsLoading(false);
    }
  };

  const logout = () => {
    localStorage.removeItem(AUTH_STORAGE_KEY);
    localStorage.removeItem(REMEMBER_STORAGE_KEY);
    sessionStorage.removeItem(AUTH_STORAGE_KEY);
    setAuthState({
      user: null,
      token: null,
      isAuthenticated: false,
    });
  };

  return (
    <AuthContext.Provider
      value={{
        user: authState.user,
        token: authState.token,
        isAuthenticated: authState.isAuthenticated,
        isLoading,
        login,
        logout,
        systemHealth,
        checkHealth,
      }}
    >
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
}
