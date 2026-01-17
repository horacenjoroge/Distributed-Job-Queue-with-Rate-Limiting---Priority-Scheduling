import React, { useState, useEffect } from 'react'
import { workerService } from '../services/api'
import websocket from '../services/websocket'

const Workers = () => {
  const [workers, setWorkers] = useState([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    loadWorkers()
    setupWebSocket()

    const interval = setInterval(loadWorkers, 5000)

    return () => {
      clearInterval(interval)
      websocket.off('worker_updated', handleWorkerUpdate)
    }
  }, [])

  const setupWebSocket = () => {
    websocket.connect()
    websocket.on('worker_updated', handleWorkerUpdate)
  }

  const handleWorkerUpdate = (data) => {
    loadWorkers()
  }

  const loadWorkers = async () => {
    try {
      setLoading(true)
      const response = await workerService.list()
      setWorkers(response.data.workers || [])
    } catch (error) {
      console.error('Error loading workers:', error)
    } finally {
      setLoading(false)
    }
  }

  const getStatusBadge = (worker) => {
    const isAlive = worker.is_alive || worker.status === 'active'
    const lastHeartbeat = worker.last_heartbeat
      ? new Date(worker.last_heartbeat)
      : null
    const now = new Date()
    const heartbeatAge = lastHeartbeat
      ? Math.floor((now - lastHeartbeat) / 1000)
      : null

    if (!isAlive || (heartbeatAge && heartbeatAge > 60)) {
      return 'bg-red-100 text-red-800'
    }
    if (heartbeatAge && heartbeatAge > 30) {
      return 'bg-yellow-100 text-yellow-800'
    }
    return 'bg-green-100 text-green-800'
  }

  const getStatusText = (worker) => {
    const isAlive = worker.is_alive || worker.status === 'active'
    const lastHeartbeat = worker.last_heartbeat
      ? new Date(worker.last_heartbeat)
      : null
    const now = new Date()
    const heartbeatAge = lastHeartbeat
      ? Math.floor((now - lastHeartbeat) / 1000)
      : null

    if (!isAlive || (heartbeatAge && heartbeatAge > 60)) {
      return 'Dead'
    }
    if (heartbeatAge && heartbeatAge > 30) {
      return 'Stale'
    }
    return worker.status || 'Active'
  }

  const formatHeartbeat = (heartbeat) => {
    if (!heartbeat) return 'Never'
    const date = new Date(heartbeat)
    const now = new Date()
    const seconds = Math.floor((now - date) / 1000)
    
    if (seconds < 10) return 'Just now'
    if (seconds < 60) return `${seconds}s ago`
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`
    return `${Math.floor(seconds / 3600)}h ago`
  }

  if (loading && workers.length === 0) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-gray-500">Loading workers...</div>
      </div>
    )
  }

  return (
    <div>
      <h1 className="text-3xl font-bold text-gray-900 mb-8">Workers</h1>

      {/* Summary Stats */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
        <div className="bg-white rounded-lg shadow-md p-6">
          <div className="text-2xl font-bold text-gray-900">{workers.length}</div>
          <div className="text-sm text-gray-500">Total Workers</div>
        </div>
        <div className="bg-white rounded-lg shadow-md p-6">
          <div className="text-2xl font-bold text-green-600">
            {workers.filter(w => w.is_alive || w.status === 'active').length}
          </div>
          <div className="text-sm text-gray-500">Active Workers</div>
        </div>
        <div className="bg-white rounded-lg shadow-md p-6">
          <div className="text-2xl font-bold text-yellow-600">
            {workers.filter(w => {
              const lastHeartbeat = w.last_heartbeat ? new Date(w.last_heartbeat) : null
              const now = new Date()
              const heartbeatAge = lastHeartbeat ? Math.floor((now - lastHeartbeat) / 1000) : null
              return heartbeatAge && heartbeatAge > 30 && heartbeatAge <= 60
            }).length}
          </div>
          <div className="text-sm text-gray-500">Stale Workers</div>
        </div>
        <div className="bg-white rounded-lg shadow-md p-6">
          <div className="text-2xl font-bold text-red-600">
            {workers.filter(w => {
              const isAlive = w.is_alive || w.status === 'active'
              const lastHeartbeat = w.last_heartbeat ? new Date(w.last_heartbeat) : null
              const now = new Date()
              const heartbeatAge = lastHeartbeat ? Math.floor((now - lastHeartbeat) / 1000) : null
              return !isAlive || (heartbeatAge && heartbeatAge > 60)
            }).length}
          </div>
          <div className="text-sm text-gray-500">Dead Workers</div>
        </div>
      </div>

      {/* Workers Table */}
      <div className="bg-white rounded-lg shadow-md overflow-hidden">
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Worker ID</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Hostname</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Status</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Queue</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Worker Type</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Active Tasks</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Last Heartbeat</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Started</th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {workers.map((worker) => (
                <tr key={worker.id} className="hover:bg-gray-50">
                  <td className="px-6 py-4 whitespace-nowrap">
                    <span className="font-mono text-sm text-gray-900">{worker.id}</span>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    {worker.hostname || 'N/A'}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <span className={`px-2 py-1 text-xs font-semibold rounded-full ${getStatusBadge(worker)}`}>
                      {getStatusText(worker)}
                    </span>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {worker.queue_name || 'default'}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {worker.worker_type || 'default'}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    {worker.active_tasks || 0}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {formatHeartbeat(worker.last_heartbeat)}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {worker.started_at ? new Date(worker.started_at).toLocaleString() : 'N/A'}
                  </td>
                </tr>
              ))}
              {workers.length === 0 && (
                <tr>
                  <td colSpan="8" className="px-6 py-4 text-center text-gray-500">
                    No workers found
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}

export default Workers
