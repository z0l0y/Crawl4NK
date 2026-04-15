# Crawl4NK (牛客网面经爬虫)

## 项目背景

之前在整理面试资料的时候经常会有查看面经，学习面经的重复过程，为了优化这样一个流程，形成固有 SOP，打算开发一个爬虫项目，爬取牛客网上的面经。

牛客网聚集了海量的求职面经，但原生网页端由于反爬虫机制、动态渲染（SSR）以及复杂多变的页面结构，导致自动化获取和整理信息较为复杂（由于近几年牛客网更新的原因，很多原有的老 API 接口与结构样式已经过时了）。同时，针对不同岗位和公司的散落面经也缺乏体系化的归类和排版，难以直接用于高效率的复习。

结合以上原因，打算启动一个关于这的爬虫项目。😀

## 项目目标

完成最基本地爬取功能，支持三种格式的导出，如 MD，XLSX 和 TXT 格式的数据收集导出。

重点体现标题，公司，搜索关键词，帖子链接，正文这五个维度的内容，其中评论及回复目前暂不支持~

## 使用方式

主要配置一下配置文件就可以愉快地玩耍了，本项目的参数并不复杂，这里就不一一赘述了。

主要就是复制一个配置文件 config.template.json，重命名为 config.json，然后把你的 cookie 放进去。可以通过 keywords 先过滤掉一些和我们这次找面经无关的内容，max_pages 和 max_items_per_keyword 分别控制可爬取的总页数和最大爬取数量，默认是 5 和 10。

output_file，output_formats 分别用于控制导出文件的前缀名和需要导出的格式有哪些。debug_log 是一个 log 开关，默认开启，建议关闭。filter_rules 是基于 keywords 的再次过滤，因为有很多广告和与面经无关的内容 --- 学习经验，offer 选择，内推信息，公司吐槽等等。通过双重过滤降低内容污染率至 5% 左右（部分帖子标题正常，但是内容是广子，这个目前也会被爬取到）。

## 成果展示

<div align="center">

基于 Pandas 导出的高度结构化 xlsx 表格 <br>
<img width="800" alt="xlsx效果展示" src="https://github.com/user-attachments/assets/e666e0c9-e4b0-4e4e-aa05-35fec12cca2b" />

<br><br>

自动生成具有良好排版阅读体验的 Markdown 文档 <br>
<img width="800" alt="md效果展示" src="https://github.com/user-attachments/assets/84b7d7d8-646d-43ad-aed7-66821fb1c60b" />

<br><br>

纯净降噪、高度还原段落换行的 TXT 阅览流 <br>
<img width="800" alt="txt效果展示" src="https://github.com/user-attachments/assets/1c64943a-0f26-4168-9627-fe818c1bd88c" />

</div>

## 后续规划

未来可能接入个定时任务，每天定时发送面经？或者结合 AI 做一个知识库？

如果项目有问题欢迎提相关 issue，后续如果有需要也会继续更新完善项目~
