// See https://aka.ms/new-console-template for more information

using System.Data;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Running;
using Bogus;
using ClickHouse.Client.Copy;
using ClickHouse.Ado;

Console.WriteLine("Hello, World!");

var summary = BenchmarkRunner.Run<ClickHouseService>(DefaultConfig.Instance.WithOptions(ConfigOptions.DisableOptimizationsValidator));
//var test = new ClickHouseService();
//test.BatchSize = 11;
//await test.ClickHouseAdo();
//
// test.BatchSize = 12;
// await test.ClickHouseClient();
// test.BatchSize = 13;
// await test.OctonicaClickHouseClient();

[MemoryDiagnoser(true)]
public class ClickHouseService
{
    [Params(1000, 10000,100000,1000000)] public int BatchSize { get; set; }
    
    // [Benchmark]
    // public async Task OctonicaClickHouseClient()
    // {
    //     using var connection = new Octonica.ClickHouseClient.ClickHouseConnection("Host=localhost;User=yowko;Port=9000;Password=pass.123;Database=test;ReadWriteTimeout=10000;CommandTimeout=10");
    //     connection.Open();
    //
    //     var orders = GetOrders(BatchSize);
    //     using var writer = connection.CreateColumnWriter("INSERT INTO orders VALUES");
    //     //await using var writer = await connection.CreateColumnWriterAsync("INSERT INTO test.orders (id,order_date,product_id,order_type,amount) values", default);
    //     //new Dictionary<string, object?>()
    //     // .Add("id",orders.Select(order=>order.Id))
    //     try
    //     {
    //        // writer.WriteTable(new Dictionary<string, object?>());
    //        writer.WriteTable(new Dictionary<string, object?>()
    //                {
    //                    {"id",orders.Select(order=>order.Id)},
    //                    {"order_date",orders.Select(order=>order.OrderDate)},
    //                    {"product_id",orders.Select(order=>(UInt32)order.ProductId)},
    //                    {"order_type",orders.Select(order=>order.OrderType)},
    //                    {"amount",orders.Select(order=>order.Amount)}
    //                }
    //        ,BatchSize);
    //         //await writer.WriteRowAsync(new object[] { orders.Select(order=>order.Id), orders.Select(order=>order.OrderDate),orders.Select(order=>(UInt32)order.ProductId),orders.Select(order=>order.OrderType),orders.Select(order=>order.Amount) }.ToArray());
    //         //await writer.WriteTableAsync(new object[] { orders.Select(order=>order.Id), orders.Select(order=>order.OrderDate),orders.Select(order=>(UInt32)order.ProductId),orders.Select(order=>order.OrderType),orders.Select(order=>order.Amount) }, orders.Length, CancellationToken.None);
    //         //orders.Select(a => new object[] { a.Id, a.OrderDate, (UInt32)a.ProductId, a.OrderType, a.Amount })
    //         //await writer.WriteTableAsync(orders.Select(a => new object[] { a.Id, a.OrderDate, (UInt32)a.ProductId, a.OrderType, a.Amount }).ToArray(), orders.Length, default);
    //     }
    //     catch (Exception e)
    //     {
    //         Console.WriteLine(e);
    //     }
    //     finally
    //     {
    //         connection.Close();
    //         //await connection.CloseAsync();
    //     }
    //
    //     Console.WriteLine("end");
    // }

    [Benchmark]
    public async Task ClickHouseClient()
    {
        const string connectionString =
            "Compression=True;Timeout=10000;Host=localhost;Port=8123;Database=test;Username=yowko;Password=pass.123";

        using var connection = new ClickHouse.Client.ADO.ClickHouseConnection(connectionString);

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = "test.orders",
            ColumnNames = new[] {"id","order_date","product_id","order_type","amount"},
            BatchSize = BatchSize
        };
        await bulkCopy.InitAsync(); // Prepares ClickHouseBulkCopy instance by loading target column types
        var orders = GetOrders(BatchSize).Select(order => new object[]
        { order.Id,order.OrderDate,order.ProductId,order.OrderType,order.Amount});
        
        try
        {
            await bulkCopy.WriteToServerAsync(orders);
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
        }
        finally
        {
            connection.Close();
        }
    }

    [Benchmark]
    public async Task ClickHouseAdo()
    {
        const string connectionString =
            "Compress=True;BufferSize=32768;SocketTimeout=10000;CheckCompressedHash=False;Compressor=lz4;Host=localhost;Port=9000;Database=test;User=yowko;Password=pass.123";
        var orders = GetOrders(BatchSize);
        
        using var connection = new ClickHouse.Ado.ClickHouseConnection(connectionString);
    
        connection.Open();
    
        var command = connection
            .CreateCommand("INSERT INTO orders (id,order_date,product_id,order_type,amount) VALUES @bulk")
            .AddParameter("bulk", DbType.Object,
                orders.Select(a => new object[] { a.Id, a.OrderDate, (UInt32)a.ProductId, a.OrderType, a.Amount }));
        try
        {
            await command.ExecuteNonQueryAsync();
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
        }
        finally
        {
            connection.Close();
        }
    }
    
    Order[] GetOrders(int BatchSize)
    {
        var startDate = new DateTime(2021, 01, 01, 00, 00, 00, DateTimeKind.Utc);
        var order = new Faker<Order>()
            .RuleFor(a => a.Id, f => f.Random.ULong())
            .RuleFor(a => a.OrderDate, f => startDate.AddDays(f.Random.Number(0, 365 * 3)))
            .RuleFor(a => a.ProductId, f => f.Random.Number(1, 10000))
            .RuleFor(a => a.OrderType, f => f.Random.Byte(1, 10))
            .RuleFor(a => a.Amount, f => f.Random.Decimal(0M, 100000M));
        return order.Generate(BatchSize).ToArray();
    }
}
public class Order
{
    public ulong Id { get; set; }
    public DateTime OrderDate { get; set; }
    public int ProductId { get; set; }
    public Byte OrderType { get; set; }
    public decimal Amount { get; set; }
}