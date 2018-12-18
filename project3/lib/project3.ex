defmodule Project3 do
  use GenServer

  def start do
    args = System.argv()

    if(length(args) == 2) do
      [numNodes, numRequests] = args
      {numNodes, _} = Integer.parse(numNodes)
      {numRequests, _} = Integer.parse(numRequests)
      initializedNodes = initializeValues(numNodes)
      makeRequests(initializedNodes, numRequests)
      totalhops = counthops(initializedNodes)
      totalrequests = numNodes * numRequests
      avghops = totalhops / totalrequests
      IO.puts("Total hops = #{totalhops}")
      IO.puts("Total requests = #{totalrequests}")
      IO.puts("Average hops = #{avghops}")
    else
      IO.puts("Incorrect input")
    end
  end

  # Calculate the value of m, create the given number of nodes and create the chord topology network with them
  def initializeValues(totalNodes) do
    m = trunc(Float.ceil(:math.log2(totalNodes)))
    createdNodes = Enum.map(0..(totalNodes - 1), fn x -> createNode(x) end)
    createChordNetwork(createdNodes, m)
  end

  # Create the nodes
  def createNode(x) do
    {:ok, pid} = GenServer.start_link(__MODULE__, :ok, [x])
    GenServer.cast(pid, {:update, x})
    GenServer.cast(pid, {:changestatus, true})
    pid
  end

  # Create the chord ring and initialize the finger table accordingly
  def createChordNetwork(nodes, m) do
    IO.puts("Creating Chord Network")
    modulo = trunc(:math.pow(2, m))

    Enum.each(nodes, fn x ->
      fingerTable =
        Enum.map(0..(m - 1), fn y ->
          id = rem(trunc(:math.pow(2, y)) + Enum.find_index(nodes, fn k -> x == k end), modulo)

          if Enum.at(nodes, id) == nil do
            [id, Enum.at(nodes, 0)]
          else
            [id, Enum.at(nodes, id)]
          end
        end)

      updateFingerTable(x, fingerTable)
    end)

    nodes
  end

  # Make the said number of requests from every node by generating a random id at the rate of 1 request/second
  def makeRequests(nodes, number) do
    IO.puts("Making #{number} requests")

    tasks =
      Enum.map(1..number, fn y ->
        IO.puts("Request number #{y}")

        task =
          Task.async(fn ->
            tasks = Enum.map(nodes, fn x ->
              randId = :rand.uniform(length(nodes))
              Task.async(fn -> find_successor(x, randId) end)
            end)
            Enum.each(tasks, fn x -> Task.await(x, 30000) end)
          end)

        :timer.sleep(1000)
        task
      end)

    Enum.each(tasks, fn x -> Task.await(x, 30000) end)
  end

  # Funtion to find the successor for a given ID
  def find_successor(node, id) do
    GenServer.cast(node, {:updatecounter})
    nodeId = GenServer.call(node, {:getId})
    successor = GenServer.call(node, {:getSuccessor})

    if id > nodeId and id <= Enum.at(successor, 0) do
      successor
    else
      nextNode = closest_preceding_node(node, id)
      find_successor(nextNode, id)
    end
  end

  # Function to find the closest preceding node in the finger table of a node for a particular ID
  def closest_preceding_node(node, id) do
    fingertable = GenServer.call(node, {:getFingerTable})
    precedingNodes = Enum.filter(fingertable, fn x -> Enum.at(x, 0) < id end)

    if precedingNodes == [] do
      Enum.at(Enum.max_by(fingertable, fn x -> Enum.at(x, 0) end), 1)
    else
      Enum.at(Enum.max_by(precedingNodes, fn x -> Enum.at(x, 0) end), 1)
    end
  end

  # Update the finger table of a given node with the said finger table data
  def updateFingerTable(pid, fingertable) do
    GenServer.cast(pid, {:addsuccessor, fingertable})
  end

  # Count the total number of hops in the network
  def counthops(nodes) do
    states = Enum.map(nodes, fn x -> GenServer.call(x, {:getstate}) end)
    Enum.reduce(states, 0, fn x, acc -> x["counter"] + acc end)
  end

  def init(:ok) do
    {:ok, %{"active" => false, "counter" => 0}}
  end

  def handle_cast({:update, x}, state) do
    {:noreply, Map.put(state, "key", x)}
  end

  def handle_cast({:updatecounter}, state) do
    counter = state["counter"]
    {:noreply, Map.put(state, "counter", counter + 1)}
  end

  def handle_cast({:addsuccessor, successorList}, state) do
    {:noreply, Map.put(state, "successors", successorList)}
  end

  def handle_cast({:changestatus, value}, state) do
    {:noreply, Map.put(state, "active", value)}
  end

  def handle_call({:getId}, _from, state) do
    {:reply, state["key"], state}
  end

  def handle_call({:getSuccessor}, _from, state) do
    {:reply, Enum.at(state["successors"], 0), state}
  end

  def handle_call({:getFingerTable}, _from, state) do
    {:reply, state["successors"], state}
  end

  def handle_call({:getstate}, _from, state) do
    {:reply, state, state}
  end
end

Project3.start()
