# 三台 VMware 虚拟机配置

这三个 `.vmx` 根据项目原集群整理，保留虚拟硬件版本 19、2 vCPU、4 GiB 内存、NAT 网卡和原磁盘控制器设置。宿主机上的 ISO 路径和原主机的 UUID、MAC 已移除，网卡地址由 VMware 生成。

| 节点 | VMware 配置 | 同目录需要的磁盘文件 |
| --- | --- | --- |
| NameNode | [namenode/hadoop.vmx](namenode/hadoop.vmx) | `hadoop.vmdk`、`hadoop-s001.vmdk` 至 `hadoop-s006.vmdk` |
| DataNode 1 | [datanode1/datanode1.vmx](datanode1/datanode1.vmx) | 该节点的 `hadoop-cl1.vmdk`、`hadoop-cl1-s001.vmdk` 至 `hadoop-cl1-s006.vmdk` |
| DataNode 2 | [datanode2/datanode2.vmx](datanode2/datanode2.vmx) | 该节点的 `hadoop-cl1.vmdk`、`hadoop-cl1-s001.vmdk` 至 `hadoop-cl1-s006.vmdk` |

`.vmx` 记录虚拟机的硬件和磁盘连接，操作系统和 HDFS 数据保存在虚拟磁盘里。已有授权的课程虚拟磁盘时，把对应的 `.vmx` 放进该节点的磁盘目录，用 VMware Workstation 打开。两台 DataNode 的磁盘文件同名，必须分别放在各自的目录。若课程镜像的 `.vmdk` 描述文件名不同，使用镜像原来的 `.vmx`，或核实匹配的磁盘后调整 `scsi0:0.fileName`。

首次启动后，按 [VMware 三节点复现指南](../../docs/vmware.md) 核对 IP、主机名、HDFS 和 YARN。没有课程磁盘的同学可以按同一份指南新建三台 Ubuntu VM 并从官方数据源重跑。
