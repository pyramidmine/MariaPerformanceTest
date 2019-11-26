using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace MariaPerformanceTest
{
	class Program
	{
		public static string ConnectionString { get; } = @"Server=192.168.0.54;Port=3306;Database=DB_EDWARD;Uid=root;Pwd=Rksvndrl!@;";
		public static string SqlUpdate { get; } = @"UPDATE MN_GR_WK_LOC SET MOD_DATE=@P2, WK_LAT_Y=@P3, WK_LNG_X=@P4, ACCURACY=@P5, GPS_USE_YN=@P6 WHERE WK_CODE=@P1;";
		public static string SqlInsert { get; } = @"INSERT INTO MN_GR_WK_LOC VALUES (@P1, @P2, @P3, @P4, @P5, @P6);";

		static readonly int NUM_WORKERS = 50000;

		static readonly int UPDATING_THREAD_COUNT = Environment.ProcessorCount;
		static List<UpdateOrInsertBenchmark> updateOrInsertBenchmarks = new List<UpdateOrInsertBenchmark>();
		static List<Thread> updateOrInsertThreads = new List<Thread>();

		static void Main(string[] args)
		{
			Timer statPrintTimer = new Timer(DoPrintStat);

			using (var cts = new CancellationTokenSource())
			{
				try
				{
					//
					// 업데이트 스레드 생성 및 실행
					//
					for (int i = 0; i < UPDATING_THREAD_COUNT; i++)
					{
						UpdateOrInsertBenchmark benchmark = new UpdateOrInsertBenchmark(i, 0, NUM_WORKERS, cts.Token);
						updateOrInsertBenchmarks.Add(benchmark);
						Thread thread = new Thread(new ThreadStart(benchmark.DoUpdateOrInsert));
						updateOrInsertThreads.Add(thread);
						thread.Start();
					}

					//
					// 통계 출력 스레드 실행
					//
					statPrintTimer.Change(1000, 1000);
				}
				catch (OperationCanceledException ex)
				{
					Console.WriteLine($"Program.Main(), Thread canceled, {ex.Message}");
				}
				catch (Exception ex)
				{
					Console.WriteLine($"{ex.Message}");
				}

				Console.ReadKey();
				cts.Cancel();
			}
		}

		static void DoPrintStat(Object state)
		{
			long[] totalStat = new long[UpdateOrInsertBenchmark.NUM_ST_TYPES];

			//
			// UPDATE/INSERT 스탯 수집 + 초기화 (매 초마다 수집된 통계 정보 출력)
			//
			foreach (var benchmark in updateOrInsertBenchmarks)
			{
				long[,] stats = new long[UpdateOrInsertBenchmark.NUM_OP_TYPES + 1, UpdateOrInsertBenchmark.NUM_ST_TYPES];
				benchmark.CollectStatistics(ref stats);
				for (int j = 0; j < UpdateOrInsertBenchmark.NUM_ST_TYPES; j++)
				{
					for (int i = 0; i < UpdateOrInsertBenchmark.NUM_OP_TYPES; i++)
					{
						stats[UpdateOrInsertBenchmark.NUM_OP_TYPES, j] += stats[i, j];
						totalStat[j] += stats[i, j];
					}
				}

				Console.WriteLine($"{DateTime.Now:HH:mm:ss}, THD:{benchmark.ThreadId}, " +
					$"UP_CN:{stats[0, 0],5}, UP_MS:{stats[0, 1],3}, " +
					$"NU_CN:{stats[1, 0],5}, NU_MS:{stats[1, 1],3}, " +
					$"IN_CN:{stats[2, 0],5}, IN_MS:{stats[2, 1],3}, " +
					$"TO_CN:{stats[3, 0],5}, TO_MS:{stats[3, 1],3}");
			}
			Console.Write($"{DateTime.Now:HH:mm:ss}, CN:");
			ConsoleColor defaultForegroundColor = Console.ForegroundColor;
			Console.ForegroundColor = ConsoleColor.Yellow;
			Console.Write($"{totalStat[0],5}");
			Console.ForegroundColor = defaultForegroundColor;
			Console.WriteLine();
		}
	}

	class UpdateOrInsertBenchmark
	{
		static readonly int OP_TYPE_UPDATED = 0;
		static readonly int OP_TYPE_NON_UPDATED = 1;
		static readonly int OP_TYPE_INSERTED = 2;
		public static readonly int NUM_OP_TYPES = 3;

		static readonly int ST_TYPE_COUNT = 0;
		static readonly int ST_TYPE_ELAPSED_MILLISECONDS = 1;
		public static readonly int NUM_ST_TYPES = 2;

		long[,] stats = new long[NUM_OP_TYPES, NUM_ST_TYPES];

		public int ThreadId { get; }
		readonly int minWorkerId;
		readonly int maxWorkerId;
		readonly CancellationToken ct;
				
		public UpdateOrInsertBenchmark(int threadId, int minWorkerId, int maxWorkerId, CancellationToken ct)
		{
			ThreadId = threadId;
			this.minWorkerId = minWorkerId;
			this.maxWorkerId = maxWorkerId;
			this.ct = ct;
		}

		public void DoUpdateOrInsert()
		{
			Console.WriteLine($"{DateTime.Now:HH:mm:ss}, DoUpdateOrInsert({ThreadId}, {minWorkerId}, {maxWorkerId}, ct) started...");

			Random rand = new Random();
			Stopwatch watch = new Stopwatch();

			try
			{
				using (MySqlConnection conn = new MySqlConnection(Program.ConnectionString))
				{
					using (MySqlCommand cmd = new MySqlCommand(Program.SqlUpdate, conn))
					{
						cmd.Parameters.Add("@P1", MySqlDbType.VarString);
						cmd.Parameters.Add("@P2", MySqlDbType.DateTime);
						cmd.Parameters.Add("@P3", MySqlDbType.VarString);
						cmd.Parameters.Add("@P4", MySqlDbType.VarString);
						cmd.Parameters.Add("@P5", MySqlDbType.VarString);
						cmd.Parameters.Add("@P6", MySqlDbType.VarString);

						conn.Open();

						bool moreToDo = true;
						while (moreToDo)
						{
							if (!this.ct.IsCancellationRequested)
							{
								//
								// 파라미터를 랜덤하게 생성
								//
								string p1 = string.Format($"W{rand.Next(minWorkerId, maxWorkerId):D5}");
								DateTime p2 = DateTime.Now;
								string p3 = string.Format($"{rand.NextDouble() * 1000:N7}");
								string p4 = string.Format($"{rand.NextDouble() * 1000:N7}");
								string p5 = string.Format($"{rand.NextDouble() * 10:N3}");
								string p6 = rand.Next() % 2 == 0 ? "Y" : "N";
								int result = 0;

								cmd.Parameters["@P1"].Value = p1;
								cmd.Parameters["@P2"].Value = p2;
								cmd.Parameters["@P3"].Value = p3;
								cmd.Parameters["@P4"].Value = p4;
								cmd.Parameters["@P5"].Value = p5;
								cmd.Parameters["@P6"].Value = p6;

								watch.Restart();
								result = cmd.ExecuteNonQuery();
								watch.Stop();

								if (0 < result)
								{
									//
									// UPDATE 된 경우
									//
									Interlocked.Increment(ref this.stats[OP_TYPE_UPDATED, ST_TYPE_COUNT]);
									Interlocked.Add(ref this.stats[OP_TYPE_UPDATED, ST_TYPE_ELAPSED_MILLISECONDS], watch.ElapsedMilliseconds);
								}
								else
								{
									//
									// UPDATE 안 된 경우
									//

									//
									// 레코드가 없어서 업데이트가 안 된 기록 추가
									//
									Interlocked.Increment(ref this.stats[OP_TYPE_NON_UPDATED, ST_TYPE_COUNT]);
									Interlocked.Add(ref this.stats[OP_TYPE_NON_UPDATED, ST_TYPE_COUNT], watch.ElapsedMilliseconds);

									//
									// INSERT 시도
									//
									using (MySqlCommand cmdInsert = new MySqlCommand(Program.SqlInsert, conn))
									{
										cmdInsert.Parameters.AddWithValue("@P1", p1);
										cmdInsert.Parameters.AddWithValue("@P2", p2);
										cmdInsert.Parameters.AddWithValue("@P3", p3);
										cmdInsert.Parameters.AddWithValue("@P4", p4);
										cmdInsert.Parameters.AddWithValue("@P5", p5);
										cmdInsert.Parameters.AddWithValue("@P6", p6);

										watch.Restart();
										result = cmdInsert.ExecuteNonQuery();
										watch.Stop();

										Interlocked.Increment(ref this.stats[OP_TYPE_INSERTED, ST_TYPE_COUNT]);
										Interlocked.Add(ref this.stats[OP_TYPE_INSERTED, ST_TYPE_COUNT], watch.ElapsedMilliseconds);
									}
								}
							}
							else
							{
								ct.ThrowIfCancellationRequested();
							}
						}
					}
				}
			}
			catch (OperationCanceledException ex)
			{
				Console.WriteLine($"DoUpdateOrInsert(), {ex.Message}");
			}
			catch (Exception ex)
			{
				Console.WriteLine($"DoUpdateOrInsert(), ThreadId={ThreadId}, {ex.Message}");
			}
		}

		public void CollectStatistics(ref long[,] stats)
		{
			//
			// 스탯 복사
			//
			for (int i = 0; i < NUM_OP_TYPES; i++)
			{
				for (int j = 0; j < NUM_ST_TYPES; j++)
				{
					stats[i, j] = Interlocked.Exchange(ref this.stats[i, j], 0);
				}
			}
		}
	}
}
