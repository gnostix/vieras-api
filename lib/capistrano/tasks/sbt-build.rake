namespace :deploy do

desc "sbt package"
task :sbt_package do
    on roles(:app), in: :sequence, wait: 5 do
      within release_path do
        execute './sbt', 'package'
      end
    end
  end
  after :published, :sbt_package

  desc "Copy api war to Jetty webapps"
  task :copy_war do
    on roles(:app), in: :sequence, wait: 5 do
      within release_path do
        #sh "cp target/scala-2.10/gnostixapi_2.10-0.1.0.war /usr/share/jetty8/webapps/."
        execute :cp, 'target/scala-2.10/gnostixapi_2.10-0.1.0.war', '/opt/jetty-8.1.15/webapps/.'
      end
    end
  end
  after :sbt_package, :copy_war

  desc "stop jetty"
  task :stop_jetty do
      on roles(:app), in: :sequence, wait: 5 do
        within release_path do
          sudo '/usr/sbin/service', 'jetty', 'stop'
        end
      end
    end
    after :published, :stop_jetty

  desc "start jetty"
  task :start_jetty do
      on roles(:app), in: :sequence, wait: 5 do
        within release_path do
           sudo '/usr/sbin/service', 'jetty', 'start'
        end
      end
   end
   after :published, :start_jetty

  end
