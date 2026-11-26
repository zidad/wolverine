using IntegrationTests;
using JasperFx.Core;
using JasperFx.Resources;
using Marten;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shouldly;
using Wolverine;
using Wolverine.Marten;

namespace MartenTests;

public class handler_that_saves_its_own_session_on_a_durable_local_queue : IAsyncLifetime
{
    private const string SchemaName = "save_own_session";

    private static CancellationToken Token => TestContext.Current.CancellationToken;

    private IHost theHost = null!;

    public async ValueTask InitializeAsync()
    {
        theHost = await Host.CreateDefaultBuilder()
            .UseWolverine(opts =>
            {
                opts.Durability.Mode = DurabilityMode.Solo;

                opts.Discovery.DisableConventionalDiscovery().IncludeType(typeof(SaveOwnSessionHandler));

                opts.Services.AddMarten(m =>
                    {
                        m.Connection(Servers.PostgresConnectionString);
                        m.DatabaseSchemaName = SchemaName;
                    })
                    .IntegrateWithWolverine(x => x.MessageStorageSchemaName = SchemaName);

                opts.Policies.AutoApplyTransactions();
                opts.PublishMessage<SaveOwnSession>().ToLocalQueue("save-own-session").UseDurableInbox();

                opts.Services.AddResourceSetupOnStartup();
            }).StartAsync(Token);

        await theHost.ResetResourceState();
    }

    public async ValueTask DisposeAsync()
    {
        await theHost.StopAsync();
        theHost.Dispose();
    }

    [Fact]
    public async Task the_chains_commit_after_the_handlers_own_save_is_persisted()
    {
        var store = theHost.Services.GetRequiredService<IDocumentStore>();
        var id = Guid.NewGuid().ToString();

        await using (var session = store.LightweightSession())
        {
            session.Store(new SaveOwnSessionJob { Id = id, State = "Pending" });
            await session.SaveChangesAsync(Token);
        }

        await theHost.MessageBus().PublishAsync(new SaveOwnSession(id));

        var deadline = DateTimeOffset.UtcNow.Add(10.Seconds());
        SaveOwnSessionJob? job;
        do
        {
            await using var query = store.QuerySession();
            job = await query.LoadAsync<SaveOwnSessionJob>(id, Token);
            if (job?.State == "Done") break;
            await Task.Delay(100, Token);
        } while (DateTimeOffset.UtcNow < deadline);

        job!.State.ShouldBe("Done");
    }
}

public record SaveOwnSession(string Id);

public class SaveOwnSessionJob
{
    public string Id { get; set; } = "";
    public string State { get; set; } = "";
}

public static class SaveOwnSessionHandler
{
    public static async Task Handle(SaveOwnSession message, IDocumentSession session, CancellationToken token)
    {
        var job = await session.LoadAsync<SaveOwnSessionJob>(message.Id, token);

        job!.State = "Running";
        session.Store(job);
        await session.SaveChangesAsync(token);

        job.State = "Done";
        session.Store(job);
    }
}
