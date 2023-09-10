package cursor

import "github.com/apache/pulsar-client-go/pulsar"

/*

Pulsar 是一个分布式发布-订阅消息系统，提供了多种消费消息的方式，其中包括 Reader 和 Consumer。以下是 Pulsar Reader 和 Consumer 的主要区别：

数据位置：Consumer 从特定的主题（Topic）中持续消费消息，每个 Consumer 从该主题的一个或多个分区中读取消息。
而 Reader 从一个或多个分区的指定位置读取消息，并且可以自由选择启动读取的位置，不需要绑定到一个特定的消费组（Consumer Group）。

提交偏移量：Consumer 可以提交已消费的消息的偏移量，以便记录其读取进度，并在 Consumer 消失或重新启动时从之前的位置继续消费。
而 Reader 无法直接提交偏移量，因为它不与消费组关联。Reader 可以使用 seek() 方法来自由控制读取的位置。

消费组：Consumer 需要与消费组相关联。在一个消费组内，每个分区只能由一个 Consumer 进行消费。这样可以确保消息的有序性和负载均衡。
而 Reader 不属于消费组，允许多个 Reader 并发读取同一主题。

消费并发性：Consumer 可以水平扩展，以允许多个 Consumer 实例并行消费分区中的消息，并提高消息吞吐量。
而 Reader 不支持水平扩展，因为它们不在消费组中，无法进行分区的负载均衡。

总的来说，Consumer 适用于需要从特定的主题和分区持续消费消息并支持偏移量记录和负载均衡的场景。
而 Reader 则适用于需要从指定位置读取消息、不需要绑定到消费组或同时支持多个 Reader 实例读取同一主题的场景。

*/

type Tracker interface {
	Last() (cp Checkpoint, err error)
	Start()
	Commit(cp Checkpoint, mid pulsar.MessageID) error
	Close()
}
