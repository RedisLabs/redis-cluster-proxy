Thread.report_on_exception = false if Thread.respond_to? :report_on_exception
setup do
    puts "SETUP"
    sleep 3
end

test 'Sleep 5' do
    log_test_update ''
    sleep 5
    log_same_line ''
end

test 'Test thread errors' do

    threads = []
    10.times.each{|i|
        t = Thread.new{
            log_test_update "Thread #{i}"
            sleep 1 + ((i / 10.0) * 2)
            raise "ERR!" if i == 5
        }
        threads << t
    }
    threads.each{|t| t.join}
    log_same_line ''
end

test 'Test thread errors (abort on except)' do

    threads = []
    10.times.each{|i|
        t = Thread.new{
            log_test_update "Thread #{i}"
            sleep 1 + ((i / 10.0) * 2)
            raise "ERR! (abort on exception)" if i == 5
        }
        t.abort_on_exception = true
        threads << t
    }
    threads.each{|t| t.join}
    log_same_line ''
end

test 'Test thread errors (1 + nil err)' do

    threads = []
    10.times.each{|i|
        t = Thread.new{
            log_test_update "Thread #{i}"
            sleep 1 + ((i / 10.0) * 2)
            a = 1 + nil if i == 5
        }
        threads << t
    }
    threads.each{|t| t.join}
    log_same_line ''
end

test 'FINAL TEST' do
    true
end
