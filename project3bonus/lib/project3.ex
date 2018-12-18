defmodule Project3 do
  use GenServer

  def start do
    args = System.argv()

    if(length(args) == 3) do
      [numNodes, numRequests, failPercentage] = args
      {numNodes, _} = Integer.parse(numNodes)
      {numRequests, _} = Integer.parse(numRequests)
      {failPercentage, _} = Integer.parse(failPercentage)
      numFailNodes = div(numNodes * failPercentage, 100)
      m = trunc(Float.ceil(:math.log2(numNodes)))
      initializedNodes = initializeValues(numNodes, m)
      initializedNodes = failNodes(initializedNodes, numFailNodes)
      task = Task.async(fn -> runStabilization(initializedNodes, m) end)
      makeRequests(initializedNodes, numRequests, m)
      Task.await(task, 30000)
      totalhops = counthops(initializedNodes)
      totalrequests = (numNodes - numFailNodes) * numRequests
      avghops = totalhops / totalrequests

      IO.puts("Total hops = #{totalhops}")
      IO.puts("Total requests = #{totalrequests}")
      IO.puts("Average hops = #{avghops}")
    else
      IO.puts("Incorrect input")
    end
  end

  # Create the given number of nodes and create the chord topology network with them
  def initializeValues(totalNodes, m) do

    createdNodes = Enum.map(0..(totalNodes - 1), fn x -> createNode(x) end)
    createChordNetwork(createdNodes, m)
  end

  # Changes the status of a particular number of nodes to false which tell us that the nodes have failed
  def failNodes(nodes, failnumber) do
    Enum.reduce(1..failnumber, nodes, fn _, acc ->
      randomID = :rand.uniform(length(acc) - 1)
      {randomNode, acc} = List.pop_at(acc, randomID)
      GenServer.cast(randomNode, {:changestatus, false})
      acc
    end)
    nodes
  end

  # Once the nodes have failed, the network is stabilised by parallely running this function in the background
  def runStabilization(nodes, m) do
    tasks =
      Enum.map(nodes, fn x ->
        Task.async(fn ->
          if GenServer.call(x, {:getstatus}) do
            successorlist = GenServer.call(x, {:getsuccessorlist})
            successor = checkIfAlive(successorlist)
            fingertable = GenServer.call(x, {:getFingerTable})

            fingertable =
              List.replace_at(fingertable, 0, successor)

            updateFingerTable(x, fingertable)
            fingertable = fixFingerTable(x, fingertable, m)
            updateFingerTable(x, fingertable)
          end
        end)
      end)

    Enum.each(tasks, fn x -> Task.await(x, 60000) end)
  end

  # Fixes the finger table by finding the successor for every entry in the finger table
  def fixFingerTable(node, fingertable, m) do
    fingerTable =
      Enum.map(fingertable, fn x ->
        successor = find_successor(node, Enum.at(x, 0), m)
        successor
      end)

    fingerTable
  end

  # Returns the first alive node in the node's consecutive successor list. Also halts the process in case all the nodes in the successor list fails
  def checkIfAlive(successorlist) do
    {successor, successorlist} = List.pop_at(successorlist, 0)

    if successor == nil do
      IO.puts(
        "Rare condition in which all successors in the successorlist failed achieved. Exiting."
      )

      Process.exit(self(), :normal)
    end

    if GenServer.call(Enum.at(successor, 1), {:getstatus}) do
      successor
    else
      checkIfAlive(successorlist)
    end
  end

  # Create the nodes
  def createNode(x) do
    {:ok, pid} = GenServer.start_link(__MODULE__, :ok, [x])
    # Add itself in the successor list
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
            [0, Enum.at(nodes, 0)]
          else
            [id, Enum.at(nodes, id)]
          end
        end)

      successorList =
        Enum.map(1..3, fn y ->
          index = Enum.find_index(nodes, fn k -> x == k end)
          id = rem(index + y, length(nodes))
          [id, Enum.at(nodes, id)]
        end)

      updateSuccessorList(x, successorList)
      updateFingerTable(x, fingerTable)
    end)

    nodes
  end

  # Make the said number of requests from every node by generating a random id at the rate of 1 request/second
  def makeRequests(nodes, number, m) do
    IO.puts("Making #{number} requests")
    nodes = Enum.filter(nodes, fn x -> GenServer.call(x, {:getstatus}) end)
    tasks =
      Enum.map(1..number, fn y ->
        IO.puts("Request number #{y}")

        task =
          Task.async(fn ->
            tasks = Enum.map(nodes, fn x ->
              randId = :rand.uniform(length(nodes))
              task = Task.async(fn -> find_successor(x, randId, m) end)
              task
            end)
            Enum.each(tasks, fn x -> Task.await(x, 30000) end)
          end)

        :timer.sleep(1000)
        task
      end)

    Enum.each(tasks, fn x -> Task.await(x, 30000) end)
  end

  # Funtion to find the successor for a given ID
  def find_successor(node, id, m) do
    GenServer.cast(node, {:updatecounter})
    nodeId = GenServer.call(node, {:getId})
    successor = GenServer.call(node, {:getSuccessor})
    successorrange = id_generator(nodeId-1, Enum.at(successor, 0), m, [])
    if Enum.member?(successorrange, id) do
      successor
    else
      nextNode = closest_preceding_node(node, id)

      if Enum.at(nextNode, 0) == GenServer.call(Enum.at(nextNode, 1), {:getId}) do
        find_successor(Enum.at(nextNode, 1), id, m)
      else
        nextNode
      end
    end
  end

  # Generates a list of numbers from first to last in the range of 2^m
  def id_generator(first, last, m, list) do
    if first == last do
      list
    else
      first = rem(first+1, trunc(:math.pow(2, m)))
      list = list ++ [first]
      id_generator(first, last, m, list)
    end
  end

  # Function to find the closest preceding node in the finger table of a node for a particular ID
  def closest_preceding_node(node, id) do
    fingertable = GenServer.call(node, {:getFingerTable})

    precedingNodes =
      Enum.filter(fingertable, fn x ->
        Enum.at(x, 0)<id
      end)

    if precedingNodes == [] do
      fingertable =
        Enum.filter(fingertable, fn x -> GenServer.call(Enum.at(x, 1), {:getstatus}) end)

      Enum.max_by(fingertable, fn x -> Enum.at(x, 0) end)
    else
      Enum.max_by(precedingNodes, fn x -> Enum.at(x, 0) end)
    end
  end

  # Update the finger table of a given node with the said finger table data
  def updateFingerTable(pid, fingertable) do
    GenServer.cast(pid, {:addsuccessor, fingertable})
  end

  # Update the successor list with 3 consecutive successors
  def updateSuccessorList(pid, successorList) do
    GenServer.cast(pid, {:updatesuccessorlist, successorList})
  end

  # Count the total number of hops in the network
  def counthops(nodes) do
    states = Enum.map(nodes, fn x -> GenServer.call(x, {:getstate}) end)
    Enum.reduce(states, 0, fn x, acc -> x["counter"] + acc end)
  end

  def init(:ok) do
    # We create nodes with no predecessor and status as inactive
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

  def handle_cast({:updatesuccessorlist, successorList}, state) do
    {:noreply, Map.put(state, "successorlist", successorList)}
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

  def handle_call({:getsuccessorlist}, _from, state) do
    {:reply, state["successorlist"], state}
  end

  def handle_call({:getFingerTable}, _from, state) do
    {:reply, state["successors"], state}
  end

  def handle_call({:getstatus}, _from, state) do
    {:reply, state["active"], state}
  end

  def handle_call({:getstate}, _from, state) do
    {:reply, state, state}
  end
end

Project3.start()
