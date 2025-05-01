# xfuse - 基于FUSE的分片文件系统

xfuse为rclone等网盘挂载程序的拓展，允许您将下载好的文件分片上传到网盘挂载的目录，在本地挂载目录实现随机读取。

## 功能特点

- 🔄 将文件分片目录挂载为文件系统
- 🚀 按需加载文件数据，无需完整下载
- 🔍 通过.torrent文件将完整文件自动分片
- 🗃️ 元数据管理和缓存机制
- 🔧 高度可配置的挂载选项

## 系统要求

- Python 3.6+
- Linux操作系统(支持FUSE)

## 快速安装

使用提供的安装脚本进行快速安装：

```bash
bash <(wget -qO- https://raw.githubusercontent.com/Fang4321/xfuse/refs/heads/main/install.sh)
```

或者通过pip手动安装：

```bash
pip install git+https://github.com/Fang4321/xfuse.git
```

## 基本使用

### 挂载文件系统

```bash
xfuse [挂载点]
```

如果未指定挂载点，系统将使用默认路径。

### 命令行参数

| 参数 | 描述 |
|------|------|
| `mountpoint` | 文件系统挂载位置 |
| `--cache` | 缓存目录路径 |
| `--piece` | 文件片段存储目录 |
| `--torrent` | 种子文件目录 |
| `--db` | 元数据数据库路径 |
| `--log-level` | 日志级别（DEBUG、INFO、WARNING、ERROR） |
| `--scanner-interval` | 扫描器检查间隔（秒） |
| `--scanner-workers` | 最大并发任务数 |
| `-o, --fuse-opt` | FUSE选项（例如：-o allow_other,default_permissions） |
| `--foreground` | 在前台运行FUSE（默认：后台） |

## 工作原理

TorrentFS利用FUSE（用户空间文件系统）技术，将种子文件的内容映射为虚拟文件系统。系统包含以下主要组件：

1. **扫描器(Scanner)**: 定期扫描种子目录，检查.torrent文件在cache目录的映射并校验完整性，如果完整则分片上传到网盘
2. **元数据数据库(MetadataDB)**: 存储种子文件的结构和元数据信息
3. **FUSE模块(TorrentFS)**: 处理文件系统操作，并按需获取数据

当用户访问挂载点中的文件时，系统只获取所需的数据块，而不是下载整个文件，从而提高效率。

## 示例用例

### 挂载到自定义目录

```bash
xfuse /mnt --torrent /path/to/torrents
```

### 使用自定义缓存位置

```bash
xfuse --cache /path/to/cache
```

### 手动将文件或目录分片上传到网盘
```bash
chmod 555 /mnt
```

### 启用额外的FUSE选项

```bash
xfuse -o allow_other,default_permissions
```

## 故障排除

如果遇到问题，请检查以下事项：

1. 确保已安装FUSE并且内核支持FUSE
2. 检查错误日志（默认位于运行目录的error.log）
3. 使用`--log-level DEBUG`运行以获取详细信息
4. 确保挂载点目录存在且有适当的权限
