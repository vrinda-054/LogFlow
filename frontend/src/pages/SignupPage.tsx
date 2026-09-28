import { useEffect, useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { useAuth } from '../auth';

export default function SignupPage() {
  const navigate = useNavigate();
  const { isAuthenticated, signup, isLoading, systemHealth } = useAuth();
  const [name, setName] = useState('');
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [error, setError] = useState('');

  useEffect(() => {
    if (isAuthenticated) navigate('/', { replace: true });
  }, [isAuthenticated, navigate]);

  const handleSubmit = async (event: React.FormEvent) => {
    event.preventDefault();
    setError('');
    if (!name.trim()) return setError('Please enter your name.');
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email.trim())) return setError('Please enter a valid email address.');
    if (password.length < 8) return setError('Password must be at least 8 characters.');
    if (password !== confirmPassword) return setError('Passwords do not match.');

    try {
      await signup(name, email, password);
      navigate('/', { replace: true });
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : 'Unable to create account.');
    }
  };

  const isOnline = systemHealth?.status === 'ok';

  return (
    <div className="auth-page">
      <div className="auth-container">
        <div className="auth-brand-block">
          <div className="auth-brand-logo">
            <div className="brand-title">LOGFLOW</div>
            <div className="brand-subtitle">Process. Scale. Recover.</div>
          </div>
          <div className="auth-status-badge" aria-label="System status">
            <span className={`dot ${isOnline ? 'green' : 'yellow'}`} />
            <span className="auth-status-label">LogFlow System</span>
            <span className={`auth-status-state ${isOnline ? 'online' : ''}`}>{isOnline ? 'ONLINE' : 'DEGRADED'}</span>
          </div>
        </div>

        <main className="auth-card">
          <div className="auth-card-header">
            <h1 className="auth-title">Create Account</h1>
            <p className="auth-subtitle">Register to access the LogFlow monitoring dashboard.</p>
          </div>
          {error && <div className="auth-error-banner" role="alert"><span className="error-icon">!</span><span>{error}</span></div>}
          <form onSubmit={handleSubmit} noValidate className="auth-form">
            <div className="form-group">
              <label htmlFor="signup-name" className="form-label">Name <span className="required-star">*</span></label>
              <input id="signup-name" className="form-input" value={name} onChange={(event) => setName(event.target.value)} autoComplete="name" disabled={isLoading} />
            </div>
            <div className="form-group">
              <label htmlFor="signup-email" className="form-label">Email <span className="required-star">*</span></label>
              <input id="signup-email" type="email" className="form-input" value={email} onChange={(event) => setEmail(event.target.value)} autoComplete="email" disabled={isLoading} />
            </div>
            <div className="form-group">
              <label htmlFor="signup-password" className="form-label">Password <span className="required-star">*</span></label>
              <input id="signup-password" type="password" className="form-input" value={password} onChange={(event) => setPassword(event.target.value)} autoComplete="new-password" disabled={isLoading} />
            </div>
            <div className="form-group">
              <label htmlFor="signup-confirm-password" className="form-label">Confirm Password <span className="required-star">*</span></label>
              <input id="signup-confirm-password" type="password" className="form-input" value={confirmPassword} onChange={(event) => setConfirmPassword(event.target.value)} autoComplete="new-password" disabled={isLoading} />
            </div>
            <button type="submit" className="primary-btn auth-submit-btn" disabled={isLoading}>
              {isLoading ? <span className="btn-loading"><span className="spinner" /> Creating account...</span> : 'Create Account'}
            </button>
          </form>
          <p className="auth-subtitle auth-link-row">Already have an account? <Link to="/login">Log in</Link></p>
        </main>
        <footer className="auth-footer"><span>LogFlow • Real-Time Log Processing</span></footer>
      </div>
    </div>
  );
}
