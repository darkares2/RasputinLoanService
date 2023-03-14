using System;
using System.Threading.Tasks;
using LoanService;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace LoanServiceTests;

public class UnitTestQueueTriggerLoanService
{
    [Fact]
    public async Task UnitTestQueueTriggerLoanServiceInvalidCommandAsync()
    {
        var sut = new QueueTriggerLoanService();
        var message = new Message
        {
            Body = "{\"command\":\"invalid\",\"user\":{\"id\":\"1\",\"name\":\"John Doe\"}}"
        };
        var loggerMock = new Mock<ILogger>();

        // RunAsync and expect ArgumentNullException
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await sut.RunAsync(message.Body, loggerMock.Object));

    }
}