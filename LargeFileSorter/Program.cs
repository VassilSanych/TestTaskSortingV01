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
	private static int _chunkSize = 100000;

	private static readonly PartsComparer? _partsComparer = new();

	/// <summary>
	///  Split the file into sorted chunks
	/// </summary>
	static void SplitIntoChunks(string inputFilePath)
	{
		using var reader = new StreamReader(inputFilePath);
		var chunkIndex = 0;
		while (!reader.EndOfStream)
		{
			WriteChunk(reader, chunkIndex);

			chunkIndex++;
			Console.WriteLine($@"Chunk created: {chunkIndex}");
		}
	}

	static void WriteChunk(StreamReader reader, int chunkIndex)
	{
		var lines = new List<LineParts>(_chunkSize);
		for (var i = 0; i < _chunkSize && !reader.EndOfStream; i++)
		{
			try
			{
				var line = reader.ReadLine();
				if (!string.IsNullOrWhiteSpace(line))
					lines.Add(new LineParts(line));
			}
			catch (Exception e)
			{
				Console.WriteLine($@"ReadLine error: {e}");
				throw;
			}
		}

		lines.Sort(_partsComparer);

		using var writer = new StreamWriter($"chunk_{chunkIndex}.txt");
		foreach (var line in lines)
			writer.WriteLine("{0}. {1}", line.Number, line.Text);
	}


	/// <summary>
	///  Merge the sorted chunks
	/// </summary>
	static void MergeSortedChunks(string outputFilePath)
	{
		var files = Directory.GetFiles(".", "chunk_*.txt");
		var sortedChunks = files.Select(file => new StreamReader(file)).ToList();
		try
		{
			using var writer = new StreamWriter(outputFilePath);
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
				writer.WriteLine("{0}. {1}", kvp.Key.Number, kvp.Key.Text);

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
			foreach (var chunk in sortedChunks)
				chunk.Close();

			foreach (var file in files)
				File.Delete(file);
		}
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