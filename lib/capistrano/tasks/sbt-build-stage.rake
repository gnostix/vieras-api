namespace :deploy do

desc "sbt package on dev"
task :sbt_package_dev do
    on roles(:dev), in: :sequence, wait: 5 do
      within release_path do
        execute './sbt', 'package'
      end
    end
  end
  after :published, :sbt_package_dev

  desc "Copy api war to Jetty webapps, on dev"
  task :copy_war_dev do
    on roles(:dev), in: :sequence, wait: 5 do
      within release_path do
        execute :cp, 'target/scala-2.10/gnostixapi_2.10-0.1.0.war', '/opt/jetty-8.1.15/webapps/.'
      end
    end
  end
  after :sbt_package_dev, :copy_war_dev

  desc "restart jetty on dev"
  task :restart_jetty_dev do
      on roles(:dev), in: :sequence, wait: 5 do
        within release_path do
          sudo '/usr/sbin/service', 'jetty', 'restart'
        end
      end
    end
    after :published, :restart_jetty_dev

  end
