/*
 The input is a large text file, where each line is a Number. String
	For example:
415. Apple
30432. Something something something
1. Apple
32. Cherry is the best
2. Banana is yellow
	Both parts can be repeated within the file. You need to get another file as output, where all
the lines are sorted. Sorting criteria: String part is compared first, if it matches then
Number.
	Those in the example above, it should be:
1. Apple
415. Apple
2. Banana is yellow
32. Cherry is the best
30432. Something something something
You need to write two programs:
1. A utility for creating a test file of a given size. The result of the work should be a text file
of the type described above. There must be some number of lines with the same String
part.
2. The actual sorter. An important point, the file can be very large. The size of ~100Gb will
be used for testing.
	When evaluating the completed task, we will first look at the result (correctness of
generation / sorting and running time), and secondly, at how the candidate writes the code.
	Programming language: C#.

*/


namespace TestTaskSorting;

internal class LargeFileSorter
{
	static long allLines = 0;
	static long chunkLines = 0;
	static long mergedLines = 0;
	static long resultLines = 0;

	static void Main(string[] args)
	{
		try
		{
			string inputFilePath = args.Length > 0
				? args[0]
				: "testfile.txt";

			string outputFilePath = args.Length > 1 && long.TryParse(args[1], out var megaBytes)
				? args[1]
				: "sortedfile.txt";

			SortLargeFile(inputFilePath, outputFilePath);
		}
		catch (Exception e)
		{
			Console.WriteLine(e);
		}
	}

	/// <summary>
	/// Number of lines per chunk
	/// </summary>
	private static int _chunkSize = 1000000;

	private static readonly PartsComparer? _partsComparer = new();

	/// <summary>
	///  Split the file into sorted chunks
	/// </summary>
	static void SplitIntoChunks(string inputFilePath)
	{
		var tasks = new Task[3];
		tasks[0] = Task.Factory.StartNew(() => MakeChunks(inputFilePath), TaskCreationOptions.LongRunning);
		tasks[1] = Task.Factory.StartNew(() => SortChunks(), TaskCreationOptions.LongRunning);
		tasks[2] = Task.Factory.StartNew(() => WriteSortedChunks(), TaskCreationOptions.LongRunning);
		Task.WaitAll(tasks);
	}


	static AutoResetEvent ChunkTakenToSortEvent = new(true);
	static AutoResetEvent ChunkFilledEvent = new(false);
	static AutoResetEvent ChunkSortedEvent = new(false);
	static AutoResetEvent ChunkTakenToWriteEvent = new(true);

	static List<LineParts>? filledLines;
	static LineParts[] sortedlines;
	static long chunkIndex = 0;



	static void MakeChunks(string inputFilePath)
	{
		using var reader = new StreamReader(inputFilePath);
		while (!reader.EndOfStream)
		{
			ChunkTakenToSortEvent.WaitOne();
			var linesToFill = new List<LineParts>(_chunkSize);
			for (var i = 0; i < _chunkSize && !reader.EndOfStream; i++)
			{
				try
				{
					var line = reader.ReadLine();
					allLines++;
					if (!string.IsNullOrWhiteSpace(line))
						linesToFill.Add(new LineParts(line));
				}
				catch (Exception e)
				{
					Console.WriteLine($@"ReadLine error: {e}");
					filledLines = null;
					ChunkFilledEvent.Set();
					throw;
				}
			}

			filledLines = linesToFill.ToList();
			ChunkFilledEvent.Set();
		}

		ChunkTakenToSortEvent.WaitOne();
		filledLines = null;
		ChunkFilledEvent.Set();
	}

	static void SortChunks()
	{
		while (true)
		{
			ChunkTakenToWriteEvent.WaitOne();
			ChunkFilledEvent.WaitOne();
			if (filledLines == null)
				break;
			Span<LineParts> linesToSort = filledLines.ToArray();
			ChunkTakenToSortEvent.Set();
			linesToSort.Sort(_partsComparer);
			sortedlines = linesToSort.ToArray();
			ChunkSortedEvent.Set();
		}

		sortedlines = null;
		ChunkSortedEvent.Set();
		ChunkTakenToSortEvent.Set();
	}

	

	static void WriteSortedChunks()
	{
		while (true)
		{
			ChunkSortedEvent.WaitOne();
			if (sortedlines == null)
				break;
			Span<LineParts> linesToWrite = sortedlines.ToArray();
			ChunkTakenToWriteEvent.Set();
			using var writer = new StreamWriter($"chunk_{chunkIndex}.txt");
			foreach (var line in linesToWrite)
			{
				writer.WriteLine("{0}. {1}", line.Number, line.Text);
				chunkLines++;
			}

			chunkIndex++;
			Console.WriteLine($@"Chunk created: {chunkIndex}");
			Console.WriteLine($@"chunkLines: {chunkLines}");
		}
		ChunkTakenToWriteEvent.Set();
	}


	static LineParts[] tempArray;

	static void GetMergedLines()
	{
		var files = Directory.GetFiles(".", "chunk_*.txt");
		var sortedChunks = files.Select(file => new StreamReader(file)).ToList();
		try
		{
			var priorityQueue = new SortedDictionary<LineParts, StreamReader>(_partsComparer);

			foreach (var chunk in sortedChunks)
			{
				if (chunk.EndOfStream)
					continue;
				var line = chunk.ReadLine();
				if (line == null)
					continue;
				var parts = new LineParts(line);
				while (!priorityQueue.TryAdd(parts, chunk))
					parts.Version++;
			}

			while (priorityQueue.Count > 0)
			{
				var kvp = priorityQueue.First();

				mergedLinesBuffer.Add(kvp.Key);
				mergedLines++;
				if (mergedLinesBuffer.Count > 10000)
				{
					tempArray = mergedLinesBuffer.ToArray();
					MergedLinePreparedEvent.Set();
					MergedLinesTakenEvent.WaitOne();
					mergedLinesBuffer.Clear();
				}


				if (!kvp.Value.EndOfStream)
				{
					var nextLine = kvp.Value.ReadLine();
					priorityQueue.Remove(kvp.Key);
					if (nextLine != null)
					{
						var parts = new LineParts(nextLine);
						while (!priorityQueue.TryAdd(parts, kvp.Value))
							parts.Version++;
					}
				}
				else
				{
					kvp.Value.Close();
					priorityQueue.Remove(kvp.Key);
				}
			}
		}
		finally
		{
			tempArray = mergedLinesBuffer.ToArray();
			MergedLinePreparedEvent.Set();
			MergedLinesTakenEvent.WaitOne();
			mergedLinesBuffer = null;
			MergedLinePreparedEvent.Set();

			foreach (var chunk in sortedChunks)
				chunk.Close();

			foreach (var file in files)
				File.Delete(file);

			Console.WriteLine($"mergedLines: {mergedLines}");
		}
	}


	static List<LineParts>? mergedLinesBuffer = new();
	static AutoResetEvent MergedLinePreparedEvent = new(false);
	static AutoResetEvent MergedLinesTakenEvent = new(true);



	static void WriteMergedLines(string outputFilePath)
	{
		using var writer = new StreamWriter(outputFilePath);
		while (true)
		{
			MergedLinePreparedEvent.WaitOne();
			if (mergedLinesBuffer == null)
				break;

			Span<LineParts> takenMergedLinesBuffer = tempArray;

			MergedLinesTakenEvent.Set();

			foreach (var line in takenMergedLinesBuffer)
			{
				writer.WriteLine("{0}. {1}", line.Number, line.Text);
				resultLines++;
			}
		}

		MergedLinesTakenEvent.Set();
		Console.WriteLine($"allLines: {allLines}");
		Console.WriteLine($"resultLines: {resultLines}");
	}


	/// <summary>
	///  Merge the sorted chunks
	/// </summary>
	static void MergeSortedChunks(string outputFilePath)
	{
		var tasks = new Task[2];
		tasks[0] = Task.Factory.StartNew(GetMergedLines, TaskCreationOptions.LongRunning);
		tasks[1] = Task.Factory.StartNew(() => WriteMergedLines(outputFilePath), TaskCreationOptions.LongRunning);
		Task.WaitAll(tasks);
	}

	static void SortLargeFile(string inputFilePath, string outputFilePath)
	{
		SplitIntoChunks(inputFilePath);
		MergeSortedChunks(outputFilePath);
	}
}

internal struct LineParts
{
	public LineParts(string line)
	{
		var parts = line.Split([". "], 2, StringSplitOptions.None);
		Number = int.Parse(parts[0]);
		Text = parts[1];
	}

	public int Number { get; }
	public string Text { get; }
	public int Version { get; set; } = 0;
}

internal class PartsComparer : IComparer<LineParts>
{
	// Compares by parts: text first, number second
	public int Compare(LineParts x, LineParts y)
	{
		var stringComparison = string.Compare(x.Text, y.Text, StringComparison.Ordinal);
		var numberAfterStringComparison = stringComparison != 0 ? stringComparison : x.Number.CompareTo(y.Number);
		return numberAfterStringComparison != 0 ? numberAfterStringComparison : x.Version.CompareTo(y.Version);
	}
}