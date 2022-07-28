import { useState } from 'react'
import './App.css'
import Paperbase from './Paperbase';

function App() {
  const [count, setCount] = useState(0)

  return (
    <div className="App">
      <Paperbase />
    </div>
  )
}

export default App
