import React, { useState, useEffect } from 'react'
import { Link } from 'react-router-dom'
import { taskService, queueService, workerService, metricsService } from '../services/api'
import websocket from '../services/websocket'

const StatCard = ({ title, value, icon, color = 'blue', link }) => {
  const content = (
    <div className={`bg-white rounded-lg shadow-md p-6 border-l-4 border-${color}-500`}>
      <div className="flex items-center justify-between">
        <div>
          <p className="text-gray-600 text-sm font-medium">{title}</p>
          <p className="text-3xl font-bold text-gray-900 mt-2">{value}</p>
        </div>
        <div className={`text-${color}-500 text-4xl`}>{icon}</div>
      </div>
    </div>
  )

  if (link) {
    return <Link to={link}>{content}</Link>
  }

  return content
}

const Home = () => {
  const [stats, setStats] = useState({
    totalQueues: 0,
    totalTasks: 0,
    activeWorkers: 0,
    successRate: 0,
    queueSizes: {},
    recentTasks: []
  })
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    loadData()
    setupWebSocket()

    // Auto-refresh every 5 seconds
    const interval = setInterval(loadData, 5000)

    return () => {
      clearInterval(interval)
      websocket.off('task_updated', handleTaskUpdate)
      websocket.off('worker_updated', handleWorkerUpdate)
    }
  }, [])

  const setupWebSocket = () => {
    websocket.connect()
    websocket.on('task_updated', handleTaskUpdate)
    websocket.on('worker_updated', handleWorkerUpdate)
  }

  const handleTaskUpdate = (data) => {
    loadData()
  }

  const handleWorkerUpdate = (data) => {
    loadData()
  }

  const loadData = async () => {
    try {
      setLoading(true)

      // Load queues
      const queuesRes = await queueService.list()
      const queues = queuesRes.data.queues || []
      const queueSizes = {}
      let totalTasks = 0
      queues.forEach(q => {
        queueSizes[q.queue_name] = q.size || 0
        totalTasks += q.size || 0
      })

      // Load workers
      const workersRes = await workerService.list()
      const workers = workersRes.data.workers || []
      const activeWorkers = workers.filter(w => w.status === 'active' || w.is_alive).length

      // Load metrics for success rate
      let successRate = 0
      try {
        const metricsRes = await metricsService.get(3600)
        const metrics = metricsRes.data
        if (metrics.success_rate !== undefined) {
          successRate = (metrics.success_rate * 100).toFixed(1)
        }
      } catch (e) {
        console.error('Error loading metrics:', e)
      }

      // Load recent tasks
      const tasksRes = await taskService.list({ limit: 10 })
      const recentTasks = tasksRes.data.tasks || []

      setStats({
        totalQueues: queues.length,
        totalTasks,
        activeWorkers,
        successRate,
        queueSizes,
        recentTasks
      })
    } catch (error) {
      console.error('Error loading dashboard data:', error)
    } finally {
      setLoading(false)
    }
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-gray-500">Loading dashboard...</div>
      </div>
    )
  }

  return (
    <div>
      <h1 className="text-3xl font-bold text-gray-900 mb-8">Dashboard Overview</h1>

      {/* Stats Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
        <StatCard
          title="Total Queues"
          value={stats.totalQueues}
          icon="📦"
          color="blue"
          link="/queues"
        />
        <StatCard
          title="Total Tasks"
          value={stats.totalTasks}
          icon="📋"
          color="green"
          link="/tasks"
        />
        <StatCard
          title="Active Workers"
          value={stats.activeWorkers}
          icon="⚙️"
          color="purple"
          link="/workers"
        />
        <StatCard
          title="Success Rate"
          value={`${stats.successRate}%`}
          icon="✅"
          color="green"
        />
      </div>

      {/* Queue Sizes */}
      <div className="bg-white rounded-lg shadow-md p-6 mb-8">
        <h2 className="text-xl font-bold text-gray-900 mb-4">Queue Sizes</h2>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {Object.entries(stats.queueSizes).map(([name, size]) => (
            <div key={name} className="flex items-center justify-between p-4 bg-gray-50 rounded-lg">
              <span className="font-medium text-gray-700">{name}</span>
              <span className="text-2xl font-bold text-blue-600">{size}</span>
            </div>
          ))}
          {Object.keys(stats.queueSizes).length === 0 && (
            <div className="text-gray-500 col-span-3 text-center py-4">No queues found</div>
          )}
        </div>
      </div>

      {/* Recent Tasks */}
      <div className="bg-white rounded-lg shadow-md p-6">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-xl font-bold text-gray-900">Recent Tasks</h2>
          <Link to="/tasks" className="text-blue-600 hover:text-blue-800 font-medium">
            View All →
          </Link>
        </div>
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">ID</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Name</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Status</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Queue</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Created</th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {stats.recentTasks.map((task) => (
                <tr key={task.id} className="hover:bg-gray-50">
                  <td className="px-6 py-4 whitespace-nowrap">
                    <Link to={`/tasks/${task.id}`} className="text-blue-600 hover:text-blue-800 font-mono text-sm">
                      {task.id.substring(0, 8)}...
                    </Link>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                    {task.name}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <span className={`px-2 py-1 text-xs font-semibold rounded-full ${
                      task.status === 'success' ? 'bg-green-100 text-green-800' :
                      task.status === 'failed' ? 'bg-red-100 text-red-800' :
                      task.status === 'running' ? 'bg-blue-100 text-blue-800' :
                      'bg-gray-100 text-gray-800'
                    }`}>
                      {task.status}
                    </span>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {task.queue_name}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {new Date(task.created_at).toLocaleString()}
                  </td>
                </tr>
              ))}
              {stats.recentTasks.length === 0 && (
                <tr>
                  <td colSpan="5" className="px-6 py-4 text-center text-gray-500">
                    No tasks found
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

export default Home
