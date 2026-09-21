import type { ReactNode } from 'react';
import { NavLink } from 'react-router-dom';
import { useAuth } from '../auth';

const navItems = [
  { label: 'Overview', path: '/', icon: '◫' },
  { label: 'Consumers', path: '/consumers', icon: '◍' },
  { label: 'DLQ Inspector', path: '/dlq', icon: '▣' },
  { label: 'Live Logs', path: '/logs', icon: '⎇' },
  { label: 'Test Scenarios', path: '/scenarios', icon: '▤' },
];

export default function DashboardShell({ children }: { children: ReactNode }) {
  const { user, logout } = useAuth();

  return (
    <div className="app-layout">
      <aside className="sidebar">
        <div className="brand-block">
          <div className="brand-title">LOGFLOW</div>
          <div className="brand-subtitle">Process. Scale. Recover.</div>
        </div>

        <nav className="nav-menu">
          {navItems.map((item) => (
            <NavLink
              key={item.path}
              to={item.path}
              className={({ isActive }) => `nav-item ${isActive ? 'active' : ''}`}
            >
              <span className="nav-icon">{item.icon}</span>
              <span>{item.label}</span>
            </NavLink>
          ))}
        </nav>

        {user && (
          <div className="user-block">
            <div className="user-info">
              <span className="user-name">{user.name}</span>
              <span className="user-role">{user.role}</span>
            </div>
            <button className="signout-btn" onClick={logout} title="Sign Out">
              Sign Out
            </button>
          </div>
        )}

        <div className="system-status">
          <div className="status-header">SYSTEM STATUS</div>
          <div className="status-row"><span className="dot green" /> Kafka <span className="online">ONLINE</span></div>
          <div className="status-row"><span className="dot green" /> API <span className="online">ONLINE</span></div>
          <div className="status-row"><span className="dot green" /> Database <span className="online">ONLINE</span></div>
        </div>
      </aside>

      <main className="main-panel">{children}</main>
    </div>
  );
}

