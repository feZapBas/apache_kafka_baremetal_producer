 module white(
    input  wire        clk,
    input  wire        enable,
    input  wire        axis_tready,

    output wire [63:0] axis_tdata,
    output wire [7:0]  axis_tkeep,
    output wire        axis_tvalid,
    output wire        axis_tlast
);

    reg [31:0] timestamp_counter = 32'd0;
    reg [31:0] frame_counter     = 32'd0;
    reg [31:0] frame_timestamp   = 32'd0;

    reg axis_tvalid_i = 1'b0;

    always @(posedge clk) begin

        /* reloj de tiempo libre */
        timestamp_counter <= timestamp_counter + 32'd1;

        if (~enable) begin
            frame_counter   <= 32'd0;
            axis_tvalid_i   <= 1'b0;
        end
        else begin

            /*
             * Preparar un nuevo evento solamente cuando
             * no hay uno pendiente.
             */
            if (~axis_tvalid_i) begin
                frame_timestamp <= timestamp_counter;
                axis_tvalid_i   <= 1'b1;
            end

            /*
             * Evento aceptado por AXI4-Stream.
             */
            else if (axis_tready) begin
                frame_counter <= frame_counter + 32'd1;
                axis_tvalid_i <= 1'b0;
            end
        end
    end

    assign axis_tdata  = {frame_counter, frame_timestamp};
    assign axis_tkeep  = 8'hFF;
    assign axis_tvalid = axis_tvalid_i;
    assign axis_tlast  = axis_tvalid_i;

endmodule