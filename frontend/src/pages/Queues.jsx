import React, { useState, useEffect } from 'react'
import { queueService } from '../services/api'
import websocket from '../services/websocket'

const Queues = () => {
  const [queues, setQueues] = useState([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    loadQueues()
    setupWebSocket()

    const interval = setInterval(loadQueues, 5000)

    return () => {
      clearInterval(interval)
      websocket.off('queue_updated', handleQueueUpdate)
    }
  }, [])

  const setupWebSocket = () => {
    websocket.connect()
    websocket.on('queue_updated', handleQueueUpdate)
  }

  const handleQueueUpdate = (data) => {
    loadQueues()
  }

  const loadQueues = async () => {
    try {
      setLoading(true)
      const response = await queueService.list()
      setQueues(response.data.queues || [])
    } catch (error) {
      console.error('Error loading queues:', error)
    } finally {
      setLoading(false)
    }
  }

  const handlePurge = async (queueName) => {
    if (window.confirm(`Are you sure you want to purge queue "${queueName}"? This will remove all pending tasks.`)) {
      try {
        await queueService.purge(queueName)
        alert('Queue purged successfully')
        loadQueues()
      } catch (error) {
        alert('Error purging queue: ' + (error.response?.data?.detail || error.message))
      }
    }
  }

  if (loading && queues.length === 0) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-gray-500">Loading queues...</div>
      </div>
    )
  }

  return (
    <div>
      <h1 className="text-3xl font-bold text-gray-900 mb-8">Queues</h1>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {queues.map((queue) => (
          <div key={queue.queue_name} className="bg-white rounded-lg shadow-md p-6">
            <div className="flex items-center justify-between mb-4">
              <h2 className="text-xl font-bold text-gray-900">{queue.queue_name}</h2>
              <span className={`px-3 py-1 text-sm font-semibold rounded-full ${
                queue.size > 100 ? 'bg-red-100 text-red-800' :
                queue.size > 50 ? 'bg-yellow-100 text-yellow-800' :
                'bg-green-100 text-green-800'
              }`}>
                {queue.size || 0} tasks
              </span>
            </div>

            <div className="space-y-2 mb-4">
              <div className="flex justify-between text-sm">
                <span className="text-gray-500">Priority:</span>
                <span className="font-medium text-gray-900">{queue.priority || 'normal'}</span>
              </div>
              {queue.rate_limit && (
                <div className="flex justify-between text-sm">
                  <span className="text-gray-500">Rate Limit:</span>
                  <span className="font-medium text-gray-900">{queue.rate_limit} tasks/min</span>
                </div>
              )}
              {queue.worker_type && (
                <div className="flex justify-between text-sm">
                  <span className="text-gray-500">Worker Type:</span>
                  <span className="font-medium text-gray-900">{queue.worker_type}</span>
                </div>
              )}
            </div>

            <button
              onClick={() => handlePurge(queue.queue_name)}
              className="w-full bg-red-600 text-white px-4 py-2 rounded-lg hover:bg-red-700 transition-colors"
            >
              Purge Queue
            </button>
          </div>
        ))}

        {queues.length === 0 && (
          <div className="col-span-3 text-center py-8 text-gray-500">
            No queues found
          </div>
        )}
      </div>

      {/* Summary Stats */}
      {queues.length > 0 && (
        <div className="mt-8 bg-white rounded-lg shadow-md p-6">
          <h2 className="text-xl font-bold text-gray-900 mb-4">Summary</h2>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div>
              <div className="text-2xl font-bold text-gray-900">{queues.length}</div>
              <div className="text-sm text-gray-500">Total Queues</div>
            </div>
            <div>
              <div className="text-2xl font-bold text-gray-900">
                {queues.reduce((sum, q) => sum + (q.size || 0), 0)}
              </div>
              <div className="text-sm text-gray-500">Total Tasks</div>
            </div>
            <div>
              <div className="text-2xl font-bold text-gray-900">
                {queues.filter(q => (q.size || 0) > 0).length}
              </div>
              <div className="text-sm text-gray-500">Active Queues</div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

export default Queues
