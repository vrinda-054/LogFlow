import { createContext, useContext, useEffect, useState, ReactNode } from 'react';
import { getCurrentUser, getHealth, HealthResponse, loginRequest, logoutRequest, signupRequest, AuthUser } from './api';

export type User = {
  id: number;
  email: string;
  name: string;
};

export type AuthState = {
  user: User | null;
  isAuthenticated: boolean;
};

export interface AuthContextType {
  user: User | null;
  isAuthenticated: boolean;
  isLoading: boolean;
  login: (email: string, pass: string, rememberMe: boolean) => Promise<void>;
  signup: (name: string, email: string, pass: string) => Promise<void>;
  logout: () => Promise<void>;
  refreshUser: () => Promise<void>;
  systemHealth: HealthResponse | null;
  checkHealth: () => Promise<HealthResponse>;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [authState, setAuthState] = useState<AuthState>({ user: null, isAuthenticated: false });

  const [isLoading, setIsLoading] = useState<boolean>(true);
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
    void refreshUser();
    void checkHealth();
    const timer = setInterval(() => void checkHealth(), 15000);
    return () => clearInterval(timer);
  }, []);

  const refreshUser = async (): Promise<void> => {
    try {
      const user = await getCurrentUser();
      setAuthState({ user, isAuthenticated: true });
    } catch {
      setAuthState({ user: null, isAuthenticated: false });
    } finally {
      setIsLoading(false);
    }
  };

  const login = async (email: string, pass: string, rememberMe: boolean): Promise<void> => {
    setIsLoading(true);
    try {
      const { user } = await loginRequest(email.trim(), pass, rememberMe);
      setAuthState({ user, isAuthenticated: true });
    } finally {
      setIsLoading(false);
    }
  };

  const signup = async (name: string, email: string, pass: string): Promise<void> => {
    setIsLoading(true);
    try {
      const { user } = await signupRequest(name.trim(), email.trim(), pass);
      setAuthState({ user, isAuthenticated: true });
    } finally {
      setIsLoading(false);
    }
  };

  const logout = async (): Promise<void> => {
    try {
      await logoutRequest();
    } finally {
      setAuthState({ user: null, isAuthenticated: false });
    }
  };

  return (
    <AuthContext.Provider
      value={{
        user: authState.user,
        isAuthenticated: authState.isAuthenticated,
        isLoading,
        login,
        signup,
        logout,
        refreshUser,
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
