import React, { useState, useEffect } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { useAuth } from '../auth';

export default function LoginPage() {
  const navigate = useNavigate();
  const location = useLocation();
  const { isAuthenticated, login, isLoading, systemHealth } = useAuth();

  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [rememberMe, setRememberMe] = useState(false);
  const [showPassword, setShowPassword] = useState(false);

  const [emailError, setEmailError] = useState('');
  const [passwordError, setPasswordError] = useState('');
  const [authError, setAuthError] = useState('');

  // Redirect if already authenticated
  useEffect(() => {
    if (isAuthenticated) {
      const from = (location.state as { from?: { pathname: string } })?.from?.pathname || '/';
      navigate(from, { replace: true });
    }
  }, [isAuthenticated, navigate, location]);

  const validateForm = (): boolean => {
    let isValid = true;
    setEmailError('');
    setPasswordError('');
    setAuthError('');

    const trimmedEmail = email.trim();
    if (!trimmedEmail) {
      setEmailError('Please enter your email.');
      isValid = false;
    } else if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(trimmedEmail) && !/^[a-zA-Z0-9._-]+$/.test(trimmedEmail)) {
      setEmailError('Please enter a valid email or username.');
      isValid = false;
    }

    if (!password) {
      setPasswordError('Please enter your password.');
      isValid = false;
    }

    return isValid;
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!validateForm()) return;

    try {
      setAuthError('');
      await login(email, password, rememberMe);
      const from = (location.state as { from?: { pathname: string } })?.from?.pathname || '/';
      navigate(from, { replace: true });
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Invalid credentials.';
      setAuthError(msg);
    }
  };

  const isServerHealthy = systemHealth?.status === 'ok';
  const isServerUnreachable = systemHealth?.status === 'unreachable';

  return (
    <div className="auth-page">
      <div className="auth-container">
        {/* Header / Branding */}
        <div className="auth-brand-block">
          <div className="auth-brand-logo">
            <div className="brand-title">LOGFLOW</div>
            <div className="brand-subtitle">Process. Scale. Recover.</div>
          </div>
          
          {/* Subtle System Status Indicator */}
          <div className="auth-status-badge" aria-label="System status">
            <span
              className={`dot ${
                isServerHealthy ? 'green' : isServerUnreachable ? 'red' : 'yellow'
              }`}
            />
            <span className="auth-status-label">LogFlow System</span>
            <span className={`auth-status-state ${isServerHealthy ? 'online' : ''}`}>
              {isServerHealthy ? 'ONLINE' : isServerUnreachable ? 'UNREACHABLE' : 'DEGRADED'}
            </span>
          </div>
        </div>

        {/* Main Login Card */}
        <main className="auth-card">
          <div className="auth-card-header">
            <h1 className="auth-title">Sign In</h1>
            <p className="auth-subtitle">
              Sign in to access the LogFlow monitoring dashboard.
            </p>
          </div>

          {/* Top Error Banner */}
          {authError && (
            <div className="auth-error-banner" role="alert">
              <span className="error-icon">⚠️</span>
              <span>{authError}</span>
            </div>
          )}

          <form onSubmit={handleSubmit} noValidate className="auth-form">
            {/* Email / Username Input */}
            <div className="form-group">
              <label htmlFor="email-input" className="form-label">
                Email or Username <span className="required-star">*</span>
              </label>
              <input
                id="email-input"
                type="text"
                className={`form-input ${emailError ? 'has-error' : ''}`}
                value={email}
                onChange={(e) => {
                  setEmail(e.target.value);
                  if (emailError) setEmailError('');
                  if (authError) setAuthError('');
                }}
                placeholder="operator@logflow.internal"
                autoComplete="username"
                disabled={isLoading}
                aria-required="true"
                aria-invalid={!!emailError}
                aria-describedby={emailError ? 'email-error' : undefined}
              />
              {emailError && (
                <span id="email-error" className="input-error-text" role="alert">
                  {emailError}
                </span>
              )}
            </div>

            {/* Password Input */}
            <div className="form-group">
              <div className="label-row">
                <label htmlFor="password-input" className="form-label">
                  Password <span className="required-star">*</span>
                </label>
              </div>
              <div className="password-input-wrap">
                <input
                  id="password-input"
                  type={showPassword ? 'text' : 'password'}
                  className={`form-input ${passwordError ? 'has-error' : ''}`}
                  value={password}
                  onChange={(e) => {
                    setPassword(e.target.value);
                    if (passwordError) setPasswordError('');
                    if (authError) setAuthError('');
                  }}
                  placeholder="••••••••••••"
                  autoComplete="current-password"
                  disabled={isLoading}
                  aria-required="true"
                  aria-invalid={!!passwordError}
                  aria-describedby={passwordError ? 'password-error' : undefined}
                />
                <button
                  type="button"
                  className="password-toggle-btn"
                  onClick={() => setShowPassword((prev) => !prev)}
                  aria-label={showPassword ? 'Hide password' : 'Show password'}
                  disabled={isLoading}
                  tabIndex={0}
                >
                  {showPassword ? 'HIDE' : 'SHOW'}
                </button>
              </div>
              {passwordError && (
                <span id="password-error" className="input-error-text" role="alert">
                  {passwordError}
                </span>
              )}
            </div>

            {/* Remember Me Checkbox */}
            <div className="form-options">
              <label htmlFor="remember-me-checkbox" className="checkbox-label">
                <input
                  id="remember-me-checkbox"
                  type="checkbox"
                  className="custom-checkbox"
                  checked={rememberMe}
                  onChange={(e) => setRememberMe(e.target.checked)}
                  disabled={isLoading}
                />
                <span>Remember me on this device</span>
              </label>
            </div>

            {/* Submit Button */}
            <button
              type="submit"
              className="primary-btn auth-submit-btn"
              disabled={isLoading}
            >
              {isLoading ? (
                <span className="btn-loading">
                  <span className="spinner" /> Authenticating...
                </span>
              ) : (
                'Sign In'
              )}
            </button>
          </form>
        </main>

        {/* Footer */}
        <footer className="auth-footer">
          <span>LogFlow • Real-Time Log Processing</span>
        </footer>
      </div>
    </div>
  );
}
