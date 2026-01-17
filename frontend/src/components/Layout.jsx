import React from 'react'
import { Link, useLocation } from 'react-router-dom'

const Layout = ({ children }) => {
  const location = useLocation()

  const isActive = (path) => location.pathname === path

  const navItems = [
    { path: '/', icon: '🏠', label: 'Home' },
    { path: '/tasks', icon: '📋', label: 'Tasks' },
    { path: '/queues', icon: '📦', label: 'Queues' },
    { path: '/workers', icon: '⚙️', label: 'Workers' },
    { path: '/dlq', icon: '⚠️', label: 'DLQ' },
    { path: '/metrics', icon: '📊', label: 'Metrics' }
  ]

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Sidebar */}
      <aside className="fixed left-0 top-0 h-full w-64 bg-gray-900 text-white shadow-lg">
        <div className="p-6">
          <h1 className="text-2xl font-bold mb-8">
            <span className="text-blue-400">Job</span> Queue
          </h1>
          <nav className="space-y-2">
            {navItems.map((item) => (
              <Link
                key={item.path}
                to={item.path}
                className={`flex items-center space-x-3 px-4 py-3 rounded-lg transition-colors ${
                  isActive(item.path)
                    ? 'bg-blue-600 text-white'
                    : 'text-gray-300 hover:bg-gray-800 hover:text-white'
                }`}
              >
                <span className="text-xl">{item.icon}</span>
                <span className="font-medium">{item.label}</span>
              </Link>
            ))}
          </nav>
        </div>
      </aside>

      {/* Main Content */}
      <main className="ml-64 p-8">
        {children}
      </main>
    </div>
  )
}

export default Layout
