using System.Text;

namespace TestTaskSorting
{
	/// <summary>
	///  1. A utility for creating a test file of a given size. The result of the work should be a text file 		
	///  of the type described above. There must be some number of lines with the same String part.
	/// </summary>
	internal class TestFileGenerator
	{
		private static void Main(string[] args)
		{
			var filePath = args.Length > 0
				? args[0]
				: "testfile.txt";

			long targetSizeInBytes = args.Length > 1 && long.TryParse(args[1], out var megaBytes)
				? megaBytes * 1024 * 1024
				: 100 * 1024 * 1024; // 1000 MB for example

			GenerateTestFile(filePath, targetSizeInBytes);
		}

		private const int MaxLines = int.MaxValue;

		private static readonly string[]? _strings = ["Apple", "Banana is yellow", "Cherry is the best", "Something something something"];
		private static Span<string> Strings => _strings;

		private static readonly Random _random = new();

		private static long _fileLength;


		private static bool TryWriteLine(StreamWriter writer, long targetSizeInBytes)
		{
			var number = _random.Next(1, MaxLines);
			var str = Strings[_random.Next(Strings.Length)];
		  var numString = number.ToString();
			var lineLength = Encoding.UTF8.GetByteCount(str) + Encoding.UTF8.GetByteCount(numString) + 4;
			_fileLength += lineLength;
			if (_fileLength > targetSizeInBytes)
				return false;

		  writer.WriteLine("{0}. {1}",numString, str);
			return true;
		}


		private static void GenerateTestFile(string filePath, long targetSizeInBytes)
		{
			try
			{
				using var writer = new StreamWriter(filePath);
				Console.WriteLine($@"{filePath} generation started for the target of {targetSizeInBytes}");

				while (TryWriteLine(writer, targetSizeInBytes)) {}

				writer.BaseStream.Flush();
				Console.WriteLine($@"{writer.BaseStream.Length} bytes done");
			}
			catch (Exception e)
			{
				Console.WriteLine(e);
				throw;
			}
		}
	}
}