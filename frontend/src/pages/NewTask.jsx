import React, { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { taskService } from '../services/api'

const NewTask = () => {
  const navigate = useNavigate()
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [formData, setFormData] = useState({
    task_name: '',
    queue_name: 'default',
    priority: 'medium',
    worker_type: 'default',
    max_retries: '',
    timeout: '',
    unique: false,
    args: '',
    kwargs: ''
  })

  const handleChange = (e) => {
    const { name, value, type, checked } = e.target
    setFormData(prev => ({
      ...prev,
      [name]: type === 'checkbox' ? checked : value
    }))
  }

  const parseJsonField = (value) => {
    if (!value || value.trim() === '') return null
    try {
      return JSON.parse(value)
    } catch (e) {
      throw new Error(`Invalid JSON: ${e.message}`)
    }
  }

  const handleSubmit = async (e) => {
    e.preventDefault()
    setLoading(true)
    setError('')

    try {
      // Parse args and kwargs
      let args = null
      let kwargs = null

      if (formData.args.trim()) {
        args = parseJsonField(formData.args)
        if (!Array.isArray(args)) {
          throw new Error('Args must be a JSON array')
        }
      }

      if (formData.kwargs.trim()) {
        kwargs = parseJsonField(formData.kwargs)
        if (typeof kwargs !== 'object' || Array.isArray(kwargs)) {
          throw new Error('Kwargs must be a JSON object')
        }
      }

      // Build request payload
      const payload = {
        task_name: formData.task_name,
        queue_name: formData.queue_name,
        priority: formData.priority,
        worker_type: formData.worker_type !== 'default' ? formData.worker_type : undefined,
        unique: formData.unique
      }

      if (args !== null) payload.args = args
      if (kwargs !== null) payload.kwargs = kwargs
      if (formData.max_retries) payload.max_retries = parseInt(formData.max_retries)
      if (formData.timeout) payload.timeout = parseInt(formData.timeout)

      // Submit task
      const response = await taskService.create(payload)
      
      // Navigate to task detail page
      navigate(`/tasks/${response.data.id}`)
    } catch (err) {
      setError(err.response?.data?.detail || err.message || 'Failed to create task')
      console.error('Error creating task:', err)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div>
      <div className="flex items-center justify-between mb-8">
        <h1 className="text-3xl font-bold text-gray-900">Create New Task</h1>
        <button
          onClick={() => navigate('/tasks')}
          className="bg-gray-200 text-gray-700 px-4 py-2 rounded-lg hover:bg-gray-300 transition-colors"
        >
          ← Back to Tasks
        </button>
      </div>

      <div className="bg-white rounded-lg shadow-md p-6">
        {error && (
          <div className="mb-6 p-4 bg-red-50 border border-red-200 rounded-lg text-red-700">
            {error}
          </div>
        )}

        <form onSubmit={handleSubmit} className="space-y-6">
          {/* Task Name */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Task Name <span className="text-red-500">*</span>
            </label>
            <input
              type="text"
              name="task_name"
              value={formData.task_name}
              onChange={handleChange}
              required
              placeholder="e.g., send_email, process_image, calculate_sum"
              className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            />
            <p className="mt-1 text-sm text-gray-500">
              The name of the task function to execute (must be registered in the worker)
            </p>
          </div>

          {/* Queue Name */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Queue Name
            </label>
            <input
              type="text"
              name="queue_name"
              value={formData.queue_name}
              onChange={handleChange}
              placeholder="default"
              className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            />
            <p className="mt-1 text-sm text-gray-500">
              The queue to submit the task to (default: "default")
            </p>
          </div>

          {/* Priority and Worker Type */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Priority
              </label>
              <select
                name="priority"
                value={formData.priority}
                onChange={handleChange}
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              >
                <option value="high">High</option>
                <option value="medium">Medium</option>
                <option value="low">Low</option>
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Worker Type
              </label>
              <select
                name="worker_type"
                value={formData.worker_type}
                onChange={handleChange}
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              >
                <option value="default">Default</option>
                <option value="cpu">CPU</option>
                <option value="io">I/O</option>
                <option value="gpu">GPU</option>
              </select>
            </div>
          </div>

          {/* Max Retries and Timeout */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Max Retries
              </label>
              <input
                type="number"
                name="max_retries"
                value={formData.max_retries}
                onChange={handleChange}
                min="0"
                placeholder="3"
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              />
              <p className="mt-1 text-sm text-gray-500">
                Maximum number of retry attempts (leave empty for default)
              </p>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Timeout (seconds)
              </label>
              <input
                type="number"
                name="timeout"
                value={formData.timeout}
                onChange={handleChange}
                min="1"
                placeholder="300"
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              />
              <p className="mt-1 text-sm text-gray-500">
                Maximum execution time in seconds (leave empty for no timeout)
              </p>
            </div>
          </div>

          {/* Unique Task */}
          <div className="flex items-center">
            <input
              type="checkbox"
              name="unique"
              id="unique"
              checked={formData.unique}
              onChange={handleChange}
              className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
            />
            <label htmlFor="unique" className="ml-2 block text-sm text-gray-700">
              Unique Task (Enable deduplication)
            </label>
          </div>
          <p className="text-sm text-gray-500 -mt-4">
            If enabled, identical tasks (same name, args, kwargs) will not be executed multiple times
          </p>

          {/* Args */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Arguments (JSON Array)
            </label>
            <textarea
              name="args"
              value={formData.args}
              onChange={handleChange}
              rows="3"
              placeholder='["arg1", "arg2", 123]'
              className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent font-mono text-sm"
            />
            <p className="mt-1 text-sm text-gray-500">
              Positional arguments as a JSON array (e.g., ["value1", "value2"])
            </p>
          </div>

          {/* Kwargs */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Keyword Arguments (JSON Object)
            </label>
            <textarea
              name="kwargs"
              value={formData.kwargs}
              onChange={handleChange}
              rows="3"
              placeholder='{"key1": "value1", "key2": 123}'
              className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent font-mono text-sm"
            />
            <p className="mt-1 text-sm text-gray-500">
              Keyword arguments as a JSON object (e.g., {'{"key": "value"}'})
            </p>
          </div>

          {/* Submit Button */}
          <div className="flex justify-end space-x-4 pt-4">
            <button
              type="button"
              onClick={() => navigate('/tasks')}
              className="px-6 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 transition-colors"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={loading || !formData.task_name}
              className="px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {loading ? 'Creating...' : 'Create Task'}
            </button>
          </div>
        </form>
      </div>

      {/* Help Section */}
      <div className="mt-6 bg-blue-50 rounded-lg p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-3">💡 Tips</h3>
        <ul className="space-y-2 text-sm text-gray-700">
          <li>• <strong>Task Name:</strong> Must match a registered task function in the worker</li>
          <li>• <strong>Args/Kwargs:</strong> Use valid JSON format. Empty fields are ignored</li>
          <li>• <strong>Priority:</strong> High priority tasks are processed first</li>
          <li>• <strong>Unique:</strong> Prevents duplicate task execution based on signature</li>
          <li>• <strong>Example Args:</strong> <code className="bg-white px-1 rounded">["user@example.com", "Welcome!"]</code></li>
          <li>• <strong>Example Kwargs:</strong> <code className="bg-white px-1 rounded">{'{"to": "user@example.com", "subject": "Welcome"}'}</code></li>
        </ul>
      </div>
    </div>
  )
}

export default NewTask
