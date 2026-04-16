# .NET 微服务 + OpenIddict + EF Core + MySQL 示例

## 1. 项目结构

- `src/AuthServer`：授权中心，使用 `OpenIddict + ASP.NET Core Identity + EF Core + MySQL`
- `src/ProductApi`：商品微服务，验证 access token
- `src/OrderApi`：订单微服务，验证 access token
- `web/vue-client`：Vue3 客户端，向 `AuthServer` 获取 token 后调用微服务

## 2. 默认端口

- `AuthServer`: `https://localhost:7036`
- `ProductApi`: `https://localhost:7105`
- `OrderApi`: `https://localhost:7200`
- `Vue Client`: `http://localhost:5173`

## 3. MySQL 数据库

先准备 3 个库：

- `auth_db`
- `product_db`
- `order_db`

默认连接字符串写在各自项目的 `appsettings.json` 中：

- `src/AuthServer/appsettings.json`
- `src/ProductApi/appsettings.json`
- `src/OrderApi/appsettings.json`

如果你的 MySQL 用户名或密码不同，直接改这三个文件。

## 4. 默认测试账号

`AuthServer` 首次启动会自动创建管理员账号：

- 用户名：`admin`
- 密码：`Admin123`

## 5. 启动顺序

### 第一步：启动授权中心

```powershell
dotnet run --project .\src\AuthServer\AuthServer.csproj
```

### 第二步：启动商品微服务

```powershell
dotnet run --project .\src\ProductApi\ProductApi.csproj
```

### 第三步：启动订单微服务

```powershell
dotnet run --project .\src\OrderApi\OrderApi.csproj
```

### 第四步：启动 Vue 客户端

```powershell
cd .\web\vue-client
npm install
npm run dev
```

## 6. token 获取流程

Vue 客户端调用 `AuthServer` 的 `/connect/token`：

```http
POST https://localhost:7036/connect/token
Content-Type: application/x-www-form-urlencoded

client_id=vue-client&grant_type=password&username=admin&password=Admin123&scope=profile%20email%20offline_access%20product_api%20order_api
```

`AuthServer` 校验用户后返回：

- `access_token`
- `refresh_token`
- `expires_in`

## 7. 微服务验证 token 流程

1. Vue 在请求头中携带 `Authorization: Bearer {access_token}`
2. `ProductApi`/`OrderApi` 使用 `OpenIddict.Validation`
3. 两个微服务通过 introspection 向 `AuthServer` 校验 token 是否有效
4. token 有效且 audience 匹配时，请求通过

## 8. 当前实现的关键点

### `AuthServer`

- 使用 `IdentityUser` 存用户
- 使用 `OpenIddict` 颁发 reference token
- 支持 `password` 和 `refresh_token` 授权模式
- 自动初始化：
  - `vue-client`
  - `product_api`
  - `order_api`
  - `product_api` scope
  - `order_api` scope

### `ProductApi`

- 独立 MySQL 库 `product_db`
- `ProductsController` 需要登录后访问
- 使用 introspection 校验 token

### `OrderApi`

- 独立 MySQL 库 `order_db`
- `OrdersController` 需要登录后访问
- 使用 introspection 校验 token

## 9. 适合生产再升级的项

当前实现为了便于你快速跑通，前端使用的是 `password grant`。生产环境建议升级为：

- Vue SPA 使用 `Authorization Code + PKCE`
- 网关统一路由和鉴权
- 微服务之间拆成更细粒度 scope，例如：
  - `product.read`
  - `product.write`
  - `order.read`
  - `order.write`
- 加入 `Docker Compose`
- 加入 `Redis` 做缓存和分布式会话控制
- 加入 `RabbitMQ/Kafka` 做订单领域异步事件

## 10. 你接下来可以怎么用

1. 先改好 MySQL 连接串
2. 启动 3 个 .NET 服务
3. 启动 Vue 客户端
4. 在页面上点击“获取 Token”
5. 再点击“创建商品 / 查询商品 / 创建订单 / 查询订单”

如果你要，我下一步可以继续直接补：

- `docker-compose.yml`
- API 网关（`YARP` 或 `Ocelot`）
- `Authorization Code + PKCE` 版本的 `AuthServer + Vue3`
- 基于领域驱动的商品/订单分层目录
